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

package com.spotify.dbeam.args;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A POJO describing how to create queries for DBeam exports.
 */
@AutoValue
public abstract class QueryBuilderArgs implements Serializable {

  public abstract String tableName();

  public abstract Optional<Integer> limit();

  public abstract Optional<String> partitionColumn();

  public abstract Optional<DateTime> partition();

  public abstract ReadablePeriod partitionPeriod();

  public abstract Optional<String> splitColumn();

  public abstract Optional<Integer> queryParallelism();

  public abstract Optional<String> evenDistribution();

  public abstract Builder builder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableName(String tableName);

    public abstract Builder setLimit(Integer limit);

    public abstract Builder setLimit(Optional<Integer> limit);

    public abstract Builder setPartitionColumn(String partitionColumn);

    public abstract Builder setPartitionColumn(Optional<String> partitionColumn);

    public abstract Builder setPartition(DateTime partition);

    public abstract Builder setPartition(Optional<DateTime> partition);

    public abstract Builder setPartitionPeriod(ReadablePeriod partitionPeriod);

    public abstract Builder setSplitColumn(String splitColumn);

    public abstract Builder setSplitColumn(Optional<String> splitColumn);

    public abstract Builder setQueryParallelism(Integer parallelism);

    public abstract Builder setQueryParallelism(Optional<Integer> queryParallelism);

    public abstract Builder setEvenDistribution(String parallelism);

    public abstract Builder setEvenDistribution(Optional<String> distribution);

    public abstract QueryBuilderArgs build();
  }

  private static Boolean checkTableName(String tableName, String tableNameRegex) {
    return tableName.matches(tableNameRegex);
  }

  private static Logger LOGGER = LoggerFactory.getLogger(QueryBuilderArgs.class);

  public static QueryBuilderArgs create(String tableName, String tableNameRegex) {
    checkArgument(tableName != null,
        "TableName cannot be null");
    checkArgument(checkTableName(tableName, tableNameRegex),
        String.format("'table' must follow {}", tableNameRegex));
    return new AutoValue_QueryBuilderArgs.Builder()
        .setTableName(tableName)
        .setPartitionPeriod(Days.ONE)
        .build();
  }

  /**
   * Create queries to be executed for the export job.
   *
   * @param connection A connection which is used to determine limits for parallel queries.
   *
   * @return A list of queries to be executed.
   *
   * @throws SQLException when it fails to find out limits for splits.
   */
  public Iterable<String> buildQueries(Connection connection)
      throws SQLException {
    checkArgument(!queryParallelism().isPresent() || splitColumn().isPresent(),
        "Cannot use queryParallelism because no column to split is specified. "
        + "Please specify column to use for splitting using --splitColumn");
    checkArgument(queryParallelism().isPresent() || !splitColumn().isPresent(),
        "argument splitColumn has no effect since --queryParallelism is not specified");
    queryParallelism().ifPresent(p -> checkArgument(p > 0,
        "Query Parallelism must be a positive number. Specified queryParallelism was %s", p));

    final String limit = this.limit().map(l -> String.format(" LIMIT %d", l)).orElse("");

    final String partitionCondition = this.partitionColumn().flatMap(
        partitionColumn ->
            this.partition().map(partition -> {
              final LocalDate datePartition = partition.toLocalDate();
              final String nextPartition = datePartition.plus(partitionPeriod()).toString();
              return String.format(" AND %s >= '%s' AND %s < '%s'",
                  partitionColumn, datePartition, partitionColumn, nextPartition);
            })
    ).orElse("");

    if (queryParallelism().isPresent() && splitColumn().isPresent()) {
      LOGGER.info("Creating parallelism queries");

      String limitWithParallelism = this.limit()
          .map(l -> String.format(" LIMIT %d", l / queryParallelism().get())).orElse("");

      String queryFormat = String
          .format("SELECT * FROM %s WHERE 1=1%s%s%s",
              this.tableName(),
              partitionCondition,
              "%s", // the split conditions
              limitWithParallelism);

      if (evenDistribution().isPresent()) {
        LOGGER.info("Creating even distribution queries");
        String dist = evenDistribution().get();

        if (dist.equalsIgnoreCase("even") || dist.equalsIgnoreCase("true")) {
          Iterable<Long> bounds = findDistributionBounds(connection, this.tableName(),
              partitionCondition, splitColumn().get());

          return queriesForEvenDistribution(bounds, splitColumn().get(), queryFormat);
        } else if (dist.equalsIgnoreCase("distinct")) {
          Iterable<Long> bounds = findDistinctDistributionBounds(connection, this.tableName(),
              partitionCondition, splitColumn().get());

          return queriesForEvenDistribution(bounds, splitColumn().get(), queryFormat);
        }

      }

      LOGGER.info("Creating straight split parallelism");

      long[] minMax = findInputBounds(connection, this.tableName(), partitionCondition,
          splitColumn().get());
      long min = minMax[0];
      long max = minMax[1];

      return queriesForBounds(min, max, queryParallelism().get(), splitColumn().get(),
          queryFormat);
    }

    return Lists.newArrayList(
        String.format("SELECT * FROM %s WHERE 1=1%s%s", this.tableName(), partitionCondition,
            limit));

  }

  /**
   * Helper function which finds the min and max limits for the given split column with the
   * partition conditions.
   *
   * @return A long array of two elements, with [0] being min and [1] being max.
   *
   * @throws SQLException when there is an exception retrieving the max and min fails.
   */
  private long[] findInputBounds(Connection connection, String tableName, String partitionCondition,
                                 String splitColumn)
      throws SQLException {
    // Generate queries to get limits of split column.
    String query = String.format(
        "SELECT min(%s) as min_s, max(%s) as max_s FROM %s WHERE 1=1%s",
        splitColumn,
        splitColumn,
        tableName,
        partitionCondition);
    long min;
    long max;
    try (Statement statement = connection.createStatement()) {
      final ResultSet
          resultSet =
          statement.executeQuery(query);
      // Check and make sure we have a record. This should ideally succeed always.
      checkState(resultSet.next(), "Result Set for Min/Max returned zero records");

      // min_s and max_s would both of the same type
      switch (resultSet.getMetaData().getColumnType(1)) {
        case Types.LONGVARBINARY:
        case Types.BIGINT:
        case Types.INTEGER:
          min = resultSet.getLong("min_s");
          max = resultSet.getLong("max_s");
          break;
        default:
          throw new IllegalArgumentException("splitColumn should be of type Integer / Long");
      }
    }

    return new long[]{ min, max };
  }

  /**
   * Groups the data in buckets so that the reads can be evenly distributed.
   *
   * @return A list of values from the partition column to split by
   *
   * @throws SQLException when there is an exception retrieving the max and min fails.
   */
  private Iterable<Long> findDistributionBounds(Connection connection, String tableName,
                                                String partitionCondition,
                                                String splitColumn) throws SQLException {
    long nrOfRows = 0;
    long maxValue = 0;
    List<Long> result = new ArrayList<>();

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(
          String.format("SELECT MAX(%s), COUNT(1) FROM %s", splitColumn, tableName));

      if (rs.next()) {
        maxValue = rs.getLong(1);
        nrOfRows = rs.getLong(2);
      } else {
        return result;
      }
    }

    try (Statement statement = connection.createStatement()) {
      final ResultSet
          resultSet = statement.executeQuery(String.format(
          "SELECT %1$s, COUNT(1) AS cnt FROM %2$s GROUP BY %1$s ORDER BY %1$s",
          splitColumn,
          tableName));

      long c = 0;
      long boundary = nrOfRows / queryParallelism().get();

      while (resultSet.next()) {
        c += resultSet.getLong(2);
        if (c > boundary) {
          result.add(resultSet.getLong(1));
          c = 0;
        }
      }

      result.add(maxValue);

      return result;

    }

  }

  private Iterable<Long> findDistinctDistributionBounds(Connection connection, String tableName,
                                                        String partitionCondition,
                                                        String splitColumn) throws SQLException {
    List<Long> distinctValues = new ArrayList<>();
    List<Long> result = new ArrayList<>();

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(
          String.format("SELECT DISTINCT %s FROM %s", splitColumn, tableName));

      while (rs.next()) {
        distinctValues.add(rs.getLong(1));
      }

      int nrOfSets = distinctValues.size();
      int bucketSize = nrOfSets / queryParallelism().get();
      int c = 0;

      for (long b : distinctValues) {
        if (c >= bucketSize) {
          result.add(b);
          c = 0;
        } else {
          c++;
        }
      }
      return result;
    }

  }

  private Iterable<String> queriesForEvenDistribution(Iterable<Long> bounds,
                                                      String splitColumn,
                                                      String queryFormat) {

    List<String> queries = new ArrayList<>();
    long prev = Long.MIN_VALUE;

    for (long b : bounds) {
      String condition = " AND %1$s >= %2$s AND %1$s < %3$s ";

      queries.add(String.format(queryFormat,
          String.format(condition, splitColumn, prev, b)));

      prev = b;
    }

    // add last partition
    queries.add(String.format(queryFormat, String.format(" AND %s >= %s", splitColumn, prev)));

    return queries;
  }

  /**
   * Given a min, max and expected queryParallelism, generate all required queries that should be
   * executed.
   */
  protected static Iterable<String> queriesForBounds(long min, long max, int parallelism,
                                                     String splitColumn,
                                                     String queryFormat) {

    // We try not to generate more than queryParallelism. Hence we don't want to loose number by
    // rounding down. Also when queryParallelism is higher than max - min, we don't want 0 queries
    long bucketSize = (long) Math.ceil((double) (max - min) / (double) parallelism);
    bucketSize =
        bucketSize == 0 ? 1 : bucketSize; // If max and min is same, we export only 1 query

    List<String> queries = new ArrayList<>(parallelism);

    String parallelismCondition;
    long i = min;
    while (i + bucketSize < max) {

      LOGGER.info("Creating query lower bound :: {}", i);

      // Include lower bound and exclude the upper bound.
      parallelismCondition =
          String.format(" AND %s >= %s AND %s < %s",
              splitColumn,
              i,
              splitColumn,
              i + bucketSize);
      queries.add(String
          .format(queryFormat, parallelismCondition));
      i = i + bucketSize;
    }

    // Add last query
    if (i + bucketSize >= max) {
      // If bucket size exceeds max, we must use max and the predicate
      // should include upper bound.
      parallelismCondition =
          String.format(" AND %s >= %s AND %s <= %s",
              splitColumn,
              i,
              splitColumn,
              max);
      queries.add(String
          .format(queryFormat, parallelismCondition));
    }

    // If queryParallelism is higher than max-min, this will generate less queries.
    // But lets never generate more queries.
    checkState(queries.size() <= parallelism,
        "Unable to generate expected number of queries for given min max.");

    return queries;
  }
}

