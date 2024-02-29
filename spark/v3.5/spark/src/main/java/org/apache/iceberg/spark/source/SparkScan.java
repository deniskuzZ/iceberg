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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.NumDeletes;
import org.apache.iceberg.spark.source.metrics.NumSplits;
import org.apache.iceberg.spark.source.metrics.ScannedDataFiles;
import org.apache.iceberg.spark.source.metrics.ScannedDataManifests;
import org.apache.iceberg.spark.source.metrics.SkippedDataFiles;
import org.apache.iceberg.spark.source.metrics.SkippedDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskScannedDataFiles;
import org.apache.iceberg.spark.source.metrics.TaskScannedDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDataFiles;
import org.apache.iceberg.spark.source.metrics.TaskSkippedDataManifests;
import org.apache.iceberg.spark.source.metrics.TaskTotalFileSize;
import org.apache.iceberg.spark.source.metrics.TaskTotalPlanningDuration;
import org.apache.iceberg.spark.source.metrics.TotalFileSize;
import org.apache.iceberg.spark.source.metrics.TotalPlanningDuration;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsMerge;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkScan implements Scan, SupportsReportStatistics, SupportsMerge {
  private static final Logger LOG = LoggerFactory.getLogger(SparkScan.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final Predicate[] pushedPredicates;
  private final String branch;
  private final Supplier<ScanReport> scanReportSupplier;

  // lazy variables
  private StructType readSchema;

  SparkScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Predicate[] pushedPredicates,
      Supplier<ScanReport> scanReportSupplier) {
    Schema snapshotSchema = SnapshotUtil.schemaFor(table, readConf.branch());
    SparkSchemaUtil.validateMetadataColumnReferences(snapshotSchema, expectedSchema);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.pushedPredicates = pushedPredicates != null ? pushedPredicates : new Predicate[0];
    this.branch = readConf.branch();
    this.scanReportSupplier = scanReportSupplier;
  }

  private static boolean equivalentPredicates(Predicate[] predicates1, Predicate[] predicates2) {
    Arrays.sort(predicates1, (a, b) -> a.hashCode() - b.hashCode());
    Arrays.sort(predicates2, (a, b) -> a.hashCode() - b.hashCode());

    return Arrays.equals(predicates1, predicates2);
  }

  @Override
  public Optional<SupportsMerge> mergeWith(SupportsMerge other, SupportsRead sparkTable) {
    if (other instanceof SparkScan) {
      Predicate[] otherPredicates = ((SparkScan) other).pushedPredicates();

      if (equivalentPredicates(pushedPredicates(), otherPredicates)) {
        SparkScanBuilder scanBuilder =
            (SparkScanBuilder) sparkTable.newScanBuilder(CaseInsensitiveStringMap.empty());
        scanBuilder.pruneColumns(readSchema().merge(other.readSchema(), caseSensitive));
        scanBuilder.pushPredicates(pushedPredicates());

        return Optional.of((SparkScan) scanBuilder.build());
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.empty();
    }
  }

  protected Table table() {
    return table;
  }

  protected String branch() {
    return branch;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected List<Expression> filterExpressions() {
    return filterExpressions;
  }

  protected Types.StructType groupingKeyType() {
    return Types.StructType.of();
  }

  protected abstract List<? extends ScanTaskGroup<?>> taskGroups();

  protected Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(
        sparkContext, table, readConf, groupingKeyType(), taskGroups(), expectedSchema, hashCode());
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkMicroBatchStream(
        sparkContext, table, readConf, expectedSchema, checkpointLocation);
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(SnapshotUtil.latestSnapshot(table, branch));
  }

  protected Statistics estimateStatistics(Snapshot snapshot) {
    return estimateStatistics(snapshot, false, null);
  }

  protected Statistics estimateStatistics(
      Snapshot snapshot, boolean useFileSize, Double adjustmentFactor) {
    Preconditions.checkArgument(
        !useFileSize || adjustmentFactor != null,
        "adjustment factor must be provided to estimate statistics using file size");

    // its a fresh table, no data
    if (snapshot == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables
    // (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpressions.isEmpty()) {
      LOG.debug(
          "Using snapshot {} metadata to estimate statistics for table {}",
          snapshot.snapshotId(),
          table.name());
      long totalRecords = totalRecords(snapshot);
      return new Stats(SparkSchemaUtil.estimateSize(readSchema(), totalRecords), totalRecords);
    }

    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = 0L;

    if (useFileSize) {
      try {
        sizeInBytes =
            taskGroups().stream()
                .flatMap(t -> t.tasks().stream())
                .map(ScanTask::asFileScanTask)
                .mapToLong(f -> (long) (adjustmentFactor.doubleValue() * f.length()))
                .sum();
      } catch (RuntimeException e) {
        sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
      }
    } else {
      sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    }

    return new Stats(sizeInBytes, rowsCount);
  }

  private long totalRecords(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    return PropertyUtil.propertyAsLong(summary, SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
  }

  @Override
  public String description() {
    String groupingKeyFieldNamesAsString =
        groupingKeyType().fields().stream()
            .map(Types.NestedField::name)
            .collect(Collectors.joining(", "));

    return String.format(
        "%s (branch=%s) [filters=%s, groupedBy=%s]",
        table(), branch(), Spark3Util.describe(filterExpressions), groupingKeyFieldNamesAsString);
  }

  @Override
  public CustomTaskMetric[] reportDriverMetrics() {
    ScanReport scanReport = scanReportSupplier != null ? scanReportSupplier.get() : null;

    if (scanReport == null) {
      return new CustomTaskMetric[0];
    }

    List<CustomTaskMetric> driverMetrics = Lists.newArrayList();
    driverMetrics.add(TaskTotalFileSize.from(scanReport));
    driverMetrics.add(TaskTotalPlanningDuration.from(scanReport));
    driverMetrics.add(TaskSkippedDataFiles.from(scanReport));
    driverMetrics.add(TaskScannedDataFiles.from(scanReport));
    driverMetrics.add(TaskSkippedDataManifests.from(scanReport));
    driverMetrics.add(TaskScannedDataManifests.from(scanReport));

    return driverMetrics.toArray(new CustomTaskMetric[0]);
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[] {
      new NumSplits(),
      new NumDeletes(),
      new TotalFileSize(),
      new TotalPlanningDuration(),
      new ScannedDataManifests(),
      new SkippedDataManifests(),
      new ScannedDataFiles(),
      new SkippedDataFiles()
    };
  }

  protected long adjustSplitSize(List<? extends ScanTask> tasks, long splitSize) {
    if (readConf.splitSizeOption() == null && readConf.adaptiveSplitSizeEnabled()) {
      long scanSize = tasks.stream().mapToLong(ScanTask::sizeBytes).sum();
      int parallelism = readConf.parallelism();
      return TableScanUtil.adjustSplitSize(scanSize, parallelism, splitSize);
    } else {
      return splitSize;
    }
  }
}
