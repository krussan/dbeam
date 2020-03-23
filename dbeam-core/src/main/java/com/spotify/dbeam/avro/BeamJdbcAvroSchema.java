/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.dbeam.avro;

import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.options.JobNameConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamJdbcAvroSchema {

  private static Logger LOGGER = LoggerFactory.getLogger(BeamJdbcAvroSchema.class);

  public static Schema createSchema(Pipeline pipeline, JdbcExportArgs args, Connection connection)
      throws Exception {
    Schema generatedSchema;
    String dbName;
    final long startTime = System.nanoTime();
    dbName = connection.getCatalog();
    generatedSchema = getAvroSchema(args, connection);
    final long elapsedTimeSchema = (System.nanoTime() - startTime) / 1000000;
    LOGGER.info("Elapsed time to schema {} seconds", elapsedTimeSchema / 1000.0);

    JobNameConfiguration.configureJobName(
        pipeline.getOptions(), dbName, args.queryBuilderArgs().tableName());
    final Counter cnt =
        Metrics.counter(BeamJdbcAvroSchema.class.getCanonicalName(),
                        "schemaElapsedTimeMs");
    pipeline
        .apply("ExposeSchemaCountersSeed",
               Create.of(Collections.singletonList(0))
                   .withType(TypeDescriptors.integers()))
        .apply("ExposeSchemaCounters",
               MapElements.into(TypeDescriptors.integers()).via(v -> {
                 cnt.inc(elapsedTimeSchema);
                 return v;
               }));
    return generatedSchema;
  }

  public static Schema getAvroSchema(JdbcExportArgs args, Connection connection)
      throws SQLException {
    return args.inputAvroSchema().orElse(generateAvroSchema(args, connection));
  }

  private static Schema generateAvroSchema(JdbcExportArgs args, Connection connection)
      throws SQLException {
    final String dbUrl = connection.getMetaData().getURL();
    final String avroDoc =
        args.avroDoc()
            .orElseGet(
                () ->
                    String.format(
                        "Generate schema from JDBC ResultSet from %s %s",
                        args.queryBuilderArgs().tableName(), dbUrl));
    return JdbcAvroSchema.createSchemaByReadingOneRow(
    connection, args.queryBuilderArgs().baseSqlQuery(),
    args.avroSchemaNamespace(), avroDoc, args.useAvroLogicalTypes());
  }

  public static Optional<Schema> parseOptionalInputAvroSchemaFile(String filename)
      throws IOException {

    if (filename == null || filename.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(parseInputAvroSchemaFile(filename));
  }

  public static Schema parseInputAvroSchemaFile(String filename) throws IOException {
    MatchResult.Metadata m = FileSystems.matchSingleFileSpec(filename);
    InputStream inputStream = Channels.newInputStream(FileSystems.open(m.resourceId()));

    return new Schema.Parser().parse(inputStream);
  }
}
