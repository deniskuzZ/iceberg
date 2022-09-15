/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestSupportsReportStatistics extends SparkTestBaseWithCatalog {

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testEstimateStatisticsUsingFileSize() {
    testEstimateStatistics(true);
  }

  @Test
  public void testEstimateStatisticsDefault() {
    testEstimateStatistics(false);
  }

  private void testEstimateStatistics(boolean useFileSize) {
    withSQLConf(
        ImmutableMap.of(SparkScanBuilder.EST_STATS_USING_FILE_SIZE, useFileSize ? "true" : "false"),
        () -> {
          try {
            sql(
                "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) USING iceberg",
                tableName);

            Dataset<Row> df =
                spark
                    .range(1, 10000)
                    .withColumn(
                        "date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id AS INT) - 1")))
                    .withColumn("ts", expr("TO_TIMESTAMP(date)"))
                    .withColumn("data", expr("CAST(date AS STRING)"))
                    .select("id", "data", "date", "ts");

            df.coalesce(1).writeTo(tableName).append();

            Table table = validationCatalog.loadTable(tableIdent);
            SparkScanBuilder scanBuilder =
                new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
            SparkScan scan = (SparkScan) scanBuilder.build();
            Statistics stats = scan.estimateStatistics();

            long expectedSizeInBytes =
                useFileSize
                    ? scan.tasks().stream()
                        .flatMap(task -> task.files().stream())
                        .mapToLong(file -> file.length())
                        .sum()
                    : SparkSchemaUtil.estimateSize(scan.readSchema(), stats.numRows().getAsLong());

            Assert.assertEquals(expectedSizeInBytes, stats.sizeInBytes().getAsLong());
          } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
