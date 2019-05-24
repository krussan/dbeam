/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.options;

import com.google.common.base.Preconditions;
import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.args.QueryBuilderArgs;
import com.spotify.dbeam.avro.JdbcQueries;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcExportArgsFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcExportArgsFactory.class);

  public static JdbcExportArgs fromPipelineOptions(PipelineOptions options)
      throws ClassNotFoundException, IOException {

    final JdbcExportPipelineOptions exportOptions = options.as(JdbcExportPipelineOptions.class);

    LOGGER.info("ExportArgs :: {}", exportOptions);


    final JdbcAvroArgs jdbcAvroArgs = JdbcAvroArgs.create(
        JdbcConnectionArgs.create(exportOptions.getConnectionUrl())
            .withUsername(exportOptions.getUsername())
            .withPassword(PasswordReader.INSTANCE.readPassword(exportOptions).orElse(null)),
        exportOptions.getFetchSize(),
        exportOptions.getAvroCodec());

    LOGGER.info("Url :: {}", jdbcAvroArgs.jdbcConnectionConfiguration().url());

    final JdbcQueries queries =
        JdbcQueries.create(jdbcAvroArgs.jdbcConnectionConfiguration().url());


    return JdbcExportArgs.create(
        jdbcAvroArgs,
        createQueryArgs(queries, exportOptions),
        exportOptions.getAvroSchemaNamespace(),
        Optional.ofNullable(exportOptions.getAvroDoc()),
        exportOptions.isUseAvroLogicalTypes()
    );
  }

  public static QueryBuilderArgs createQueryArgs(JdbcQueries queries,
                                                 JdbcExportPipelineOptions options) {
    final ReadablePeriod partitionPeriod = Optional.ofNullable(options.getPartitionPeriod())
        .map(v -> (ReadablePeriod) Period.parse(v)).orElse(Days.ONE);
    Optional<DateTime> partition = Optional.ofNullable(options.getPartition())
        .map(JdbcExportArgsFactory::parseDateTime);
    Optional<String> partitionColumn = Optional.ofNullable(options.getPartitionColumn());
    Preconditions.checkArgument(
        !partitionColumn.isPresent() || partition.isPresent(),
        "To use --partitionColumn the --partition parameter must also be configured");

    if (!(options.isSkipPartitionCheck() || partitionColumn.isPresent())) {
      DateTime minPartitionDateTime = Optional.ofNullable(options.getMinPartitionPeriod())
          .map(JdbcExportArgsFactory::parseDateTime)
          .orElse(DateTime.now().minus(partitionPeriod.toPeriod().multipliedBy(2)));
      partition.map(p -> validatePartition(p, minPartitionDateTime));
    }
    return QueryBuilderArgs.create(options.getTable(), queries.getTableNameRegex())
        .builder()
        .setLimit(Optional.ofNullable(options.getLimit()))
        .setPartitionColumn(partitionColumn)
        .setPartition(partition)
        .setPartitionPeriod(partitionPeriod)
        .setSplitColumn(Optional.ofNullable(options.getSplitColumn()))
        .setQueryParallelism(Optional.ofNullable(options.getQueryParallelism()))
        .setEvenDistribution(Optional.ofNullable(options.getEvenDistribution()))
        .build();
  }

  private static DateTime parseDateTime(String input) {
    if (input.endsWith("Z")) {
      input = input.substring(0, input.length() - 1);
    }
    return DateTime.parse(input, ISODateTimeFormat.localDateOptionalTimeParser());
  }

  private static DateTime validatePartition(
      DateTime partitionDateTime, DateTime minPartitionDateTime) {
    Preconditions.checkArgument(
        partitionDateTime.isAfter(minPartitionDateTime),
        "Too old partition date %s. Use a partition date >= %s or use --skip-partition-check",
        partitionDateTime, minPartitionDateTime
    );
    return partitionDateTime;
  }

}
