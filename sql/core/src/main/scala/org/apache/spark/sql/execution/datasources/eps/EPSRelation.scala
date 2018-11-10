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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.execution.datasources.eps.{EPSPartition, EPSRDD}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

private[sql] case class EPSTableLocation(tablePart: String,
                                         dbPart: String)

private[sql] case class EPSPartitioningInfo(
                                              column: String,
                                              lowerBound: Long,
                                              upperBound: Long,
                                              numPartitions: Int,
                                              tableLocations: Array[EPSTableLocation])

private[sql] object EPSRelation extends Logging {
  def columnPartition(partitioning: EPSPartitioningInfo): Array[Partition] = {
    if (partitioning.column == null) {
      var i: Int = 0
      var ans = new ArrayBuffer[Partition]()
      for (loc <- partitioning.tableLocations) {
        ans += EPSPartition(null, loc, i)
        i = i + 1
      }
      return ans.toArray
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; Lower bound: $lowerBound; " +
          s"Upper bound: $upperBound.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions
    val locationSize = partitioning.tableLocations.length
    val column = partitioning.column
    var i: Int = 0
    var currentValue: Long = lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val uBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      var j: Int = 0
      for (loc <- partitioning.tableLocations) {
        ans += EPSPartition(whereClause, loc, i * locationSize + j)
        j = j + 1
      }
      i = i + 1
    }
    ans.toArray
  }
}

private[sql] case class EPSRelation(parts: Array[Partition],
      jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  override val schema: StructType = EPSRDD.resolveTable(jdbcOptions,
                                        parts(0).asInstanceOf[EPSPartition])

  // Check if EPSRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(EPSRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    EPSRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val url = jdbcOptions.url
    val table = jdbcOptions.table
    val properties = jdbcOptions.asProperties
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(url, table, properties)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"EPSRelation(${jdbcOptions.table})" + partitioningInfo
  }
}