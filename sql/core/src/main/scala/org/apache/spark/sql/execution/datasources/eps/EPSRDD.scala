/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.eps

import java.sql.{Connection, Date, PreparedStatement, ResultSet, SQLException, Timestamp}

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

case class EPSPartition(whereClause: String,
                        tableLocation: EPSTableLocation,
                        idx: Int) extends Partition {
  override def index: Int = idx
}

object EPSRDD extends Logging {

  def getEPSSqlSuffix(part: EPSPartition): String = {
    var tablePart = part.tableLocation.tablePart
    if (StringUtils.isEmpty(tablePart)) {
      tablePart = "null"
    }

    "/@ts=" + tablePart + "; sdb=" + part.tableLocation.dbPart + "@/"
  }

  def resolveTable(options: JDBCOptions,
                   firstPart: EPSPartition): StructType = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(
        s"${dialect.getSchemaQuery(table)} ${getEPSSqlSuffix(firstPart)}")
      try {
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, dialect)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  def compileFilter(f: Filter, dialect: JdbcDialect): Option[String] = {
    def quote(colName: String): String = dialect.quoteIdentifier(colName)

    Option(f match {
      case EqualTo(attr, value) => s"${quote(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${compileValue(value)} OR $col IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR ($col IS NULL AND ${compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%${value}'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%${value}%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${compileValue(value)})"
      case Not(f) => compileFilter(f, dialect).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def scanTable(
                 sc: SparkContext,
                 schema: StructType,
                 requiredColumns: Array[String],
                 filters: Array[Filter],
                 parts: Array[Partition],
                 options: JDBCOptions): RDD[InternalRow] = {
    val url = options.url
    val dialect = JdbcDialects.get(url)
    val quotedColumns = requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    new EPSRDD(
      sc,
      JdbcUtils.createConnectionFactory(options),
      pruneSchema(schema, requiredColumns),
      quotedColumns,
      filters,
      parts,
      url,
      options)
  }
}

private[sql] class EPSRDD(
                             sc: SparkContext,
                             getConnection: () => Connection,
                             schema: StructType,
                             columns: Array[String],
                             filters: Array[Filter],
                             partitions: Array[Partition],
                             url: String,
                             options: JDBCOptions)
  extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = partitions

  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "1" else sb.substring(1)
  }

  private val filterWhereClause: String =
  filters
    .flatMap(EPSRDD.compileFilter(_, JdbcDialects.get(url)))
    .map(p => s"($p)").mkString(" AND ")

  private def getWhereClause(part: EPSPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    var conn: Connection = null

    def close() {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => logWarning("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
      closed = true
    }

    context.addTaskCompletionListener{ context => close() }

    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[EPSPartition]
    conn = getConnection()
    val dialect = JdbcDialects.get(url)
    import scala.collection.JavaConverters._
    dialect.beforeFetch(conn, options.asConnectionProperties.asScala.toMap)

    // H2's EPS driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = getWhereClause(part)

    val cdsSuffix = EPSRDD.getEPSSqlSuffix(part)

    val sqlText = s"SELECT $columnList FROM ${options.table} $myWhereClause $cdsSuffix"
    logInfo(sqlText)

    stmt = conn.prepareStatement(sqlText,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    rs = stmt.executeQuery()
    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())
  }
}
