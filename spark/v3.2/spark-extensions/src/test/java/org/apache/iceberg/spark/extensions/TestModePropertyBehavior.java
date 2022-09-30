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
package org.apache.iceberg.spark.extensions;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/** Tests for CDPD-45218 */
public class TestModePropertyBehavior extends SparkExtensionsTestBase {

  private static final String SNAPSHOT_TABLE_NAME = "default.icebergtesttable";

  private File snapshotLocation;

  public TestModePropertyBehavior(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", SNAPSHOT_TABLE_NAME);
    snapshotLocation.delete();
  }

  @Before
  public void initSnapshotFolder() throws IOException {
    this.snapshotLocation = temp.newFolder();
  }

  @Test
  public void testCreateV2NoModeSpecified() throws Exception {
    // no explicit mode provided
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));

    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testCreateV2DeleteModeSpecified() throws Exception {
    // explicit mode provided for Delete
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testCreateV2UpdateModeSpecified() throws Exception {
    // explicit mode provided for update
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.DELETE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testCreateV2UpdateModeAsMergeOnReadSpecified() throws Exception {
    // explicit mode provided for update
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()));

    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()),
        ImmutableSet.of(TableProperties.DELETE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testCreateV2MergeModeSpecified() throws Exception {
    // explicit mode provided for merge
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    checkTableProperties(
        tableName,
        ImmutableMap.of(TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.DELETE_MODE, TableProperties.UPDATE_MODE));
  }

  @Test
  public void testCreateV2AllModesSpecified() throws Exception {
    // explicit mode provided for all
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testCreateV2DeleteUpdateModesSpecified() throws Exception {
    // explicit mode provided for Delete & Update
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()));

    checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        ImmutableSet.of(TableProperties.MERGE_MODE));
  }

  @Test
  public void testUpgradeNoModeSpecified() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1)));
    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testUpgradeUpdateModePresent() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1),
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.DELETE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testUpgradeDeleteModePresent() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1),
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testUpgradeWithDeleteModeAsPart() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1)));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testUpgradeWithDeleteModeAsPartAsMergeOnRead() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1)));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testUpgradeMergeModePresent() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1),
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.DELETE_MODE));
  }

  @Test
  public void testUpgradeAllModesPresent() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_1)));

    this.setProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName(),
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testV2SetProperty() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2),
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    this.setProperties(tableName, ImmutableMap.of("write.spark.fanout.enabled", "true"));

    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testV2UnsetProperty() throws Exception {
    this.createIcebergTable(
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, String.valueOf(TableProperties.FORMAT_VERSION_2)));

    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());

    // Now unset properties
    this.unsetProperties(
        tableName,
        ImmutableSet.of(
            "write.spark.fanout.enabled",
            TableProperties.DELETE_MODE,
            TableProperties.MERGE_MODE,
            TableProperties.UPDATE_MODE));

    this.checkTableProperties(
        tableName,
        Collections.emptyMap(),
        ImmutableSet.of(
            TableProperties.DELETE_MODE,
            TableProperties.UPDATE_MODE,
            TableProperties.MERGE_MODE,
            "write.spark.fanout.enabled"));

    // run another alter command, it should not add any explicit mode property
    this.setProperties(tableName, ImmutableMap.of("write.spark.fanout.enabled", "true"));

    this.checkTableProperties(
        tableName,
        Collections.emptyMap(),
        ImmutableSet.of(
            TableProperties.DELETE_MODE, TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testMigrateToV2NoModeSpecified() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // migrate to iceberg v2
    sql("CALL spark_catalog.system.migrate('%s', map('format-version', '2'))", tableName);
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testMigrateToV2ModeSpecified() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // migrate to iceberg v2
    sql(
        "CALL spark_catalog.system.migrate('%s', map('format-version', '2', '%s', '%s'))",
        tableName, TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.DELETE_MODE));
  }

  @Test
  public void testMigrateToV1NoModeSpecified() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // migrate to iceberg v1
    sql("CALL spark_catalog.system.migrate('%s', map('format-version', '1'))", tableName);
    this.checkTableProperties(
        tableName,
        Collections.emptyMap(),
        ImmutableSet.of(
            TableProperties.UPDATE_MODE, TableProperties.DELETE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testMigrateToV1ModeSpecified() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // migrate to iceberg v1
    sql(
        "CALL spark_catalog.system.migrate('%s', map('format-version', '1', '%s', '%s'))",
        tableName, TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.DELETE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testMigrateToIcebergDefaultVersionWithModeAbsent() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // migrate to iceberg v2 (default)
    sql("CALL spark_catalog.system.migrate('%s')", tableName);
    this.checkTableProperties(
        tableName,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testIcebergV2SnapshotWithModeAbsent() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    sql(
        "CALL spark_catalog.system.snapshot('%s', '%s', '%s', map('format-version', '2'))",
        tableName, SNAPSHOT_TABLE_NAME, this.snapshotLocation.getAbsolutePath());
    this.checkTableProperties(
        SNAPSHOT_TABLE_NAME,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  @Test
  public void testIcebergV1SnapshotWithModeAbsent() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    sql(
        "CALL spark_catalog.system.snapshot('%s', '%s', '%s', map('format-version', '1'))",
        tableName, SNAPSHOT_TABLE_NAME, this.snapshotLocation.getAbsolutePath());
    this.checkTableProperties(
        SNAPSHOT_TABLE_NAME,
        Collections.emptyMap(),
        ImmutableSet.of(
            TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE, TableProperties.DELETE_MODE));
  }

  @Test
  public void testIcebergV2SnapshotWithModePresent() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    sql(
        "CALL spark_catalog.system.snapshot('%s', '%s', '%s', map('format-version', '2', '%s', '%s'))",
        tableName,
        SNAPSHOT_TABLE_NAME,
        this.snapshotLocation.getAbsolutePath(),
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.COPY_ON_WRITE.modeName());
    this.checkTableProperties(
        SNAPSHOT_TABLE_NAME,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()),
        ImmutableSet.of(TableProperties.UPDATE_MODE, TableProperties.MERGE_MODE));
  }

  @Test
  public void testIcebergDefaultSnapshotWithModeAbsent() throws Exception {
    Assume.assumeTrue(catalogName.equals(SparkCatalogConfig.SPARK.catalogName()));
    this.createParquetTable();
    // by default, v2 table is created
    sql(
        "CALL spark_catalog.system.snapshot('%s', '%s', '%s')",
        tableName, SNAPSHOT_TABLE_NAME, this.snapshotLocation.getAbsolutePath());
    this.checkTableProperties(
        SNAPSHOT_TABLE_NAME,
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()),
        Collections.emptySet());
  }

  private void createIcebergTable(Map<String, String> tableProps) {
    final String baseIcebergTableCreationString =
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (data) ";
    final String tableString;
    if (!tableProps.isEmpty()) {
      StringBuilder tableBuilder =
          new StringBuilder(baseIcebergTableCreationString).append(" TBLPROPERTIES").append('(');
      String tblproperties =
          tableProps.entrySet().stream()
              .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
              .collect(Collectors.joining(","));
      tableBuilder.append(tblproperties).append(')');
      tableString = tableBuilder.toString();
    } else {
      tableString = baseIcebergTableCreationString;
    }
    sql(tableString, this.tableName);
  }

  private void createParquetTable() throws IOException {
    final String parquetTableStr =
        "CREATE TABLE %s (id bigint, data string) STORED AS parquet LOCATION '%s'";
    File newLocation = temp.newFolder();
    String location = newLocation.toString();
    sql(parquetTableStr, tableName, location);
  }

  private void setProperties(String tableName, Map<String, String> tablePropsToSet) {
    if (!tablePropsToSet.isEmpty()) {
      StringBuilder baseBuilder = new StringBuilder("ALTER TABLE %s SET TBLPROPERTIES").append('(');
      String tblproperties =
          tablePropsToSet.entrySet().stream()
              .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
              .collect(Collectors.joining(","));
      baseBuilder.append(tblproperties).append(')');
      sql(baseBuilder.toString(), tableName);
    }
  }

  private void unsetProperties(String tableName, Set<String> propsToUnset) {
    if (!propsToUnset.isEmpty()) {
      StringBuilder baseBuilder =
          new StringBuilder("ALTER TABLE %s UNSET TBLPROPERTIES").append('(');
      String propsStr =
          propsToUnset.stream().map(x -> String.format("'%s'", x)).collect(Collectors.joining(","));
      baseBuilder.append(propsStr).append(')');
      sql(baseBuilder.toString(), tableName);
    }
  }

  private void checkTableProperties(
      String tableName, Map<String, String> expectedProps, Set<String> absentKeys)
      throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Map<String, String> props = table.properties();
    expectedProps.forEach(
        (expectedKey, expectedvalue) ->
            Assertions.assertThat(props.get(expectedKey)).isEqualTo(expectedvalue));
    absentKeys.forEach(absentKey -> Assertions.assertThat(props).doesNotContainKey(absentKey));
  }
}
