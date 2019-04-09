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
 *//*-
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
package com.spotify.dbeam.args

import com.google.auto.value.AutoValue
import com.google.common.base.Preconditions
import java.io.Serializable
import java.sql.Connection
import java.sql.DriverManager
import javax.annotation.Nullable
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * A POJO describing a how to create a JDBC {@link Connection}.
  */
@AutoValue object JdbcConnectionArgs {
  private val LOGGER = LoggerFactory.getLogger(classOf[JdbcConnectionArgs])

  @AutoValue.Builder abstract private[args] class Builder {
    private[args] def setDriverClassName(driverClassName: String)

    private[args] def setUrl(url: String)

    private[args] def setUsername(username: String)

    private[args] def setPassword(password: String)

    private[args] def build
  }

  @throws[ClassNotFoundException]
  def create(url: String): JdbcConnectionArgs = {
    Preconditions.checkArgument(url != null, "DataSourceConfiguration.create(driverClassName, url) called " + "with null url")
    val driverClassName = JdbcConnectionUtil.getDriverClass(url)
    new Nothing().setDriverClassName(driverClassName).setUrl(url).build
  }
}

@AutoValue abstract class JdbcConnectionArgs extends Serializable {
  def driverClassName: String

  def url: String

  @Nullable private[args] def username

  @Nullable private[args] def password

  private[args] def builder

  def withUsername(username: String): JdbcConnectionArgs = builder.setUsername(username).build

  def withPassword(password: String): JdbcConnectionArgs = builder.setPassword(password).build

  @throws[Exception]
  def createConnection: Connection = {
    Class.forName(driverClassName)
    JdbcConnectionArgs.LOGGER.info("Creating JDBC connection to {} with user {}", url, username)

    val connection = DriverManager.getConnection(url, username, password)
    connection.setAutoCommit(false)
    connection
  }
}