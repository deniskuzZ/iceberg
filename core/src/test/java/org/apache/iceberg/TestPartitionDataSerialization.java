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
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestPartitionDataSerialization {
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

  private static Stream<Arguments> serializers() {
    return Stream.of(
        Arguments.of("java", (RoundTrip) TestHelpers::roundTripSerialize),
        Arguments.of("kryo", (RoundTrip) TestHelpers.KryoHelpers::roundTripSerialize));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("serializers")
  public void testDataFileRoundTrip(String serializer, RoundTrip roundTrip)
      throws IOException, ClassNotFoundException {
    List<DataFile> files = plannedFiles(table());

    assertThat(files).isNotEmpty();
    for (DataFile file : files) {
      DataFile copy = roundTrip.apply(file);
      assertEquivalent(file, copy);
      assertEquivalent(file, roundTrip.apply(copy));
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("serializers")
  public void testDeserializedDataFileWritesManifest(String serializer, RoundTrip roundTrip)
      throws IOException, ClassNotFoundException {
    Table table = table();
    List<DataFile> files = Lists.newArrayList();
    for (DataFile file : plannedFiles(table)) {
      files.add(roundTrip.apply(file));
    }

    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            2, SPEC, Files.localOutput(new File(temp, "manifest-" + serializer + ".avro")), 1L);
    try (ManifestWriter<DataFile> closing = writer) {
      files.forEach(closing::add);
    }

    List<StructLike> partitions = Lists.newArrayList();
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(writer.toManifestFile(), table.io(), table.specs())) {
      reader.forEach(file -> partitions.add(((PartitionData) file.partition()).copy()));
    }

    assertThat(partitions).containsExactlyElementsOf(Lists.transform(files, DataFile::partition));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("serializers")
  public void testPartitionDataKeepsTheSchemaItWasReadWith(String serializer, RoundTrip roundTrip)
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

    PartitionData copy = roundTrip.apply(partition);

    assertThat(copy).isEqualTo(partition);
    assertThat(copy.getSchema()).isEqualTo(avroSchema);
    assertThat(copy.copy().getSchema()).isEqualTo(avroSchema);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("serializers")
  public void testPartitionDataDerivesTheSchemaOfItsPartitionType(
      String serializer, RoundTrip roundTrip) throws IOException, ClassNotFoundException {
    PartitionData partition = new PartitionData(SPEC.partitionType());
    partition.set(0, 3);
    partition.set(1, 19724);
    partition.set(2, 40);

    for (PartitionData copy :
        ImmutableList.of(roundTrip.apply(partition), roundTrip.apply(partition.copy()))) {
      assertThat(copy).isEqualTo(partition);
      assertThat(copy.getSchema()).isEqualTo(partition.getSchema());
      assertThat(copy.getSchema())
          .isEqualTo(PartitionData.partitionDataSchema(SPEC.partitionType()));
    }
  }

  private static void assertEquivalent(DataFile expected, DataFile actual) {
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.partition()).isEqualTo(expected.partition());
    assertThat(((PartitionData) actual.partition()).getSchema())
        .isEqualTo(((PartitionData) expected.partition()).getSchema());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
  }

  private static List<DataFile> plannedFiles(Table table) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file()));
    }
    return files;
  }

  private Table table() {
    Table table = TestTables.create(temp, "partition_data", SCHEMA, SPEC, 2);
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
              .build());
    }
    append.commit();
    return table;
  }
}
