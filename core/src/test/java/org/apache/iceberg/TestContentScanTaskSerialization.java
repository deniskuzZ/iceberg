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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestContentScanTaskSerialization {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "station", Types.StringType.get()),
          optional(2, "ts", Types.TimestampType.withoutZone()),
          optional(3, "sensor_type", Types.IntegerType.get()),
          optional(4, "measure", Types.DecimalType.of(5, 2)));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA)
          .bucket("station", 11)
          .day("ts")
          .truncate("sensor_type", 10)
          .build();

  @TempDir private File temp;

  @AfterEach
  public void clearTables() {
    TestTables.clearTables();
  }

  private interface RoundTrip {
    <T> T apply(T value) throws IOException, ClassNotFoundException;
  }

  private static Stream<Arguments> filtersAndSerializers() {
    List<Expression> filters =
        ImmutableList.of(
            Expressions.alwaysTrue(),
            Expressions.greaterThan("measure", new BigDecimal("10.00")),
            Expressions.and(
                Expressions.greaterThanOrEqual("ts", "2024-01-02T00:00:00"),
                Expressions.equal("station", "s1")));
    List<Arguments> arguments = Lists.newArrayList();
    for (Expression filter : filters) {
      arguments.add(Arguments.of(filter, "java", (RoundTrip) TestHelpers::roundTripSerialize));
      arguments.add(
          Arguments.of(filter, "kryo", (RoundTrip) TestHelpers.KryoHelpers::roundTripSerialize));
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{1}: {0}")
  @MethodSource("filtersAndSerializers")
  public void testFileScanTaskRoundTrip(Expression filter, String serializer, RoundTrip roundTrip)
      throws IOException, ClassNotFoundException {
    Table table = table();
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
        // a split task serializes the file task it splits
        Iterables.addAll(tasks, task.split(16L << 20));
      }
    }

    assertThat(tasks).isNotEmpty();
    for (FileScanTask task : tasks) {
      FileScanTask copy = roundTrip.apply(task);
      assertEquivalent(task, copy);
      // a copy serializes the residual of its file, which another round trip keeps
      assertEquivalent(task, roundTrip.apply(copy));
    }
  }

  @ParameterizedTest(name = "{1}: {0}")
  @MethodSource("filtersAndSerializers")
  public void testDeserializedDataFileWritesManifest(
      Expression filter, String serializer, RoundTrip roundTrip)
      throws IOException, ClassNotFoundException {
    Table table = table();
    List<DataFile> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask task : planned) {
        files.add(roundTrip.apply(task).file());
      }
    }

    File manifestFile = new File(temp, "manifest-" + serializer + "-" + files.size() + ".avro");
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(2, SPEC, Files.localOutput(manifestFile), 1L);
    try (ManifestWriter<DataFile> closing = writer) {
      files.forEach(closing::add);
    }

    List<StructLike> partitions = Lists.newArrayList();
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(writer.toManifestFile(), table.io(), table.specs())) {
      reader.forEach(file -> partitions.add(((PartitionData) file.partition()).copy()));
    }

    assertThat(partitions)
        .containsExactlyElementsOf(Lists.transform(files, file -> file.partition()));
  }

  @Test
  public void testPartitionDataKeepsTheSchemaItWasReadWith()
      throws IOException, ClassNotFoundException {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("r102")
            .fields()
            .name("id_bucket")
            .prop("field-id", 1000)
            .type()
            .optional()
            .intType()
            .endRecord();
    PartitionData partition = new PartitionData(avroSchema);
    partition.set(0, 7);

    PartitionData javaCopy = TestHelpers.roundTripSerialize(partition);
    PartitionData kryoCopy = TestHelpers.KryoHelpers.roundTripSerialize(partition);

    assertThat(javaCopy.getSchema()).isEqualTo(avroSchema);
    assertThat(kryoCopy.getSchema()).isEqualTo(avroSchema);
    assertThat(javaCopy).isEqualTo(partition);
    assertThat(kryoCopy).isEqualTo(partition);
  }

  @Test
  public void testPartitionDataDerivesTheSchemaOfItsPartitionType()
      throws IOException, ClassNotFoundException {
    PartitionData partition = new PartitionData(SPEC.partitionType());
    partition.set(0, 3);
    partition.set(1, 19724);
    partition.set(2, 40);

    for (PartitionData copy :
        ImmutableList.of(
            TestHelpers.roundTripSerialize(partition),
            TestHelpers.KryoHelpers.roundTripSerialize(partition),
            partition.copy())) {
      assertThat(copy).isEqualTo(partition);
      assertThat(copy.getSchema()).isEqualTo(partition.getSchema());
      assertThat(copy.getSchema())
          .isEqualTo(PartitionData.partitionDataSchema(SPEC.partitionType()));
    }
  }

  private static void assertEquivalent(FileScanTask expected, FileScanTask actual) {
    assertThat(actual.file().location()).isEqualTo(expected.file().location());
    assertThat(actual.file().partition()).isEqualTo(expected.file().partition());
    assertThat(((PartitionData) actual.file().partition()).getSchema())
        .isEqualTo(((PartitionData) expected.file().partition()).getSchema());
    assertThat(actual.start()).isEqualTo(expected.start());
    assertThat(actual.length()).isEqualTo(expected.length());
    assertThat(actual.spec()).isEqualTo(expected.spec());
    assertThat(actual.schema().asStruct()).isEqualTo(expected.schema().asStruct());
    assertThat(actual.residual().toString()).isEqualTo(expected.residual().toString());
  }

  private Table table() {
    Table table = TestTables.create(temp, "serialization", SCHEMA, SPEC, 2);
    AppendFiles append = table.newFastAppend();
    for (int index = 0; index < 22; index++) {
      append.appendFile(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-" + index + ".parquet")
              .withPartitionPath(
                  String.format(
                      "station_bucket=%d/ts_day=2024-01-0%d/sensor_type_trunc=%d",
                      index % 11, 1 + index % 3, index % 2 * 10))
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(64L << 20)
              .withRecordCount(1_000_000)
              .withSplitOffsets(ImmutableList.of(4L, 32L << 20))
              .build());
    }
    append.commit();
    return table;
  }
}
