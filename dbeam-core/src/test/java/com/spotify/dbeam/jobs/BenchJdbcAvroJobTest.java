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

package com.spotify.dbeam.jobs;

import com.google.common.collect.Lists;

import com.spotify.dbeam.DbTestHelper;
import com.spotify.dbeam.TestHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BenchJdbcAvroJobTest {

  private static String CONNECTION_URL =
      "jdbc:h2:mem:test2;MODE=PostgreSQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
  private static File DIR = TestHelper.createTmpDirName("bench-jdbc-avro-test-").toFile();

  private List<String> listDir(File dir) {
    return Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .map(File::getName).sorted().collect(Collectors.toList());
  }

  @BeforeClass
  public static void beforeAll() throws SQLException, ClassNotFoundException {
    DbTestHelper.createFixtures(CONNECTION_URL);
  }

  @AfterClass
  public static void afterAll() throws IOException {
    Files.walk(DIR.toPath())
        .sorted(Comparator.reverseOrder())
        .forEach(p -> p.toFile().delete());
  }

  @Test
  public void shouldRunJdbcAvroJob() {
    BenchJdbcAvroJob.main(new String[]{
        "--targetParallelism=1",  // no need for more threads when testing
        "--skipPartitionCheck",
        "--connectionUrl=" + CONNECTION_URL,
        "--username=",
        "--table=COFFEES",
        "--output=" + DIR.getAbsolutePath(),
        "--avroCodec=zstandard1",
        "--executions=2"
    });
    Assert.assertThat(
        listDir(DIR),
        Matchers.is(
            Lists.newArrayList("run_0", "run_1")
        ));
  }

}
