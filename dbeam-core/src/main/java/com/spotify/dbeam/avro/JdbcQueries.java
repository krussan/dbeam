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

public abstract class JdbcQueries {
  public abstract String getOneRowSql();

  public abstract String getTableNameRegex();

  public static JdbcQueries create(String url) {
    final String[] parts = url.split(":", 3);

    if (parts[1].equalsIgnoreCase("sqlserver")) {
      return new SqlServerQueries();
    } else {
      return new MysqlQueries();
    }
  }
}
