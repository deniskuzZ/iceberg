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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Test;

public class TestSubqueryReuse extends SparkTestBaseWithCatalog {

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSubqueryReuse() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP, value BIGINT, partition BIGINT) USING iceberg",
        tableName);

    spark
        .range(1, 100)
        .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
        .withColumn("ts", expr("TO_TIMESTAMP(date)"))
        .withColumn("data", expr("CAST(date AS STRING)"))
        .withColumn("value", expr("id"))
        .withColumn("partition", expr("CAST(id % 10 AS INT)"))
        .select("id", "data", "date", "ts", "value", "partition")
        .writeTo(tableName)
        .append();

    String query =
        "SELECT CAST((SELECT sum(id) FROM %s WHERE partition < 5) AS INT), "
            + "CAST((SELECT sum(id) FROM %s WHERE partition >= 5) AS INT), "
            + "CAST((SELECT sum(value) FROM %s WHERE partition < 5) AS INT), "
            + "CAST((SELECT sum(value) FROM %s WHERE partition >= 5) AS INT)";

    // The result of the above query is a single row of sums
    List<Integer[]> rows = new java.util.ArrayList<>();
    rows.add(new Integer[] {2350, 2600, 2350, 2600});

    // Create a temp table with the result
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD =
        sparkContext.parallelize(rows).map((Integer[] row) -> RowFactory.create(row));
    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("foe1", DataTypes.IntegerType, false),
              DataTypes.createStructField("foe2", DataTypes.IntegerType, false),
              DataTypes.createStructField("foe3", DataTypes.IntegerType, false),
              DataTypes.createStructField("foe4", DataTypes.IntegerType, false)
            });
    spark.sqlContext().createDataFrame(rowRDD, schema).toDF().registerTempTable("df1_table");

    spark.sql("SET spark.sql.adaptive.enabled=false");

    SparkPlan plan =
        spark
            .sql(String.format(query, tableName, tableName, tableName, tableName))
            .queryExecution()
            .executedPlan();
    assert (StringUtils.countMatches(plan.toJSON(), "ReusedSubqueryExec") == 2);

    assertEquals(
        "Should have expected rows",
        sql(query, tableName, tableName, tableName, tableName),
        sql("select * from df1_table"));
  }
}
