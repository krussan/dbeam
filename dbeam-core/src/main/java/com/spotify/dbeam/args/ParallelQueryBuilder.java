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

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ParallelQueryBuilder implements Serializable {

  /**
   * Helper function which finds the min and max limits for the given split column with the
   * partition conditions.
   *
   * @return A long array of two elements, with [0] being min and [1] being max.
   * @throws SQLException when there is an exception retrieving the max and min fails.
   */
  static long[] findInputBounds(
          Connection connection, QueryBuilder queryBuilder, String splitColumn)
      throws SQLException {
    String minColumnName = "min_s";
    String maxColumnName = "max_s";
    String limitsQuery = queryBuilder.generateQueryToGetLimitsOfSplitColumn(
            splitColumn, minColumnName, maxColumnName).build();
    long min;
    long max;
    try (Statement statement = connection.createStatement()) {
      final ResultSet
          resultSet =
          statement.executeQuery(limitsQuery);
      // Check and make sure we have a record. This should ideally succeed always.
      checkState(resultSet.next(), "Result Set for Min/Max returned zero records");

      // minColumnName and maxColumnName would be both of the same type
      switch (resultSet.getMetaData().getColumnType(1)) {
        case Types.LONGVARBINARY:
        case Types.BIGINT:
        case Types.INTEGER:
          min = resultSet.getLong(minColumnName);
          // TODO
          // check resultSet.wasNull(); NULL -> 0L
          // there is no value to carry on since it will be empty set anyway 
          max = resultSet.getLong(maxColumnName);
          break;
        default:
          throw new IllegalArgumentException("splitColumn should be of type Integer / Long");
      }
    }

    return new long[]{min, max};
  }

  static Iterable<Long> findDistinctDistributionBounds(Connection connection,
                                                       int parallelism,
                                                       QueryBuilder queryBuilder,
                                                       String splitColumn) throws SQLException {
    List<Long> distinctValues = new ArrayList<>();
    List<Long> result = new ArrayList<>();

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(
              queryBuilder.generateQueryToDistinctDistributionBounds(splitColumn)
                      .build());

      while (rs.next()) {
        distinctValues.add(rs.getLong(1));
      }

      int nrOfSets = distinctValues.size();
      int bucketSize = nrOfSets / parallelism;
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

  /**
   * Groups the data in buckets so that the reads can be evenly distributed.
   *
   * @return A list of values from the partition column to split by
   *
   * @throws SQLException when there is an exception retrieving the max and min fails.
   */
  static Iterable<Long> findDistributionBounds(Connection connection,
                                               int parallelism,
                                               QueryBuilder queryBuilder,
                                               String splitColumn) throws SQLException {
    long nrOfRows = 0;
    long maxValue = 0;
    List<Long> result = new ArrayList<>();

    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(queryBuilder.generateDistributionQuery(splitColumn)
                      .build());

      if (rs.next()) {
        maxValue = rs.getLong(1);
        nrOfRows = rs.getLong(2);
      } else {
        return result;
      }
    }

    try (Statement statement = connection.createStatement()) {
      final ResultSet resultSet =
            statement.executeQuery(queryBuilder.generateDistributionBucketQuery(splitColumn)
              .build());

      long c = 0;
      long boundary = nrOfRows / parallelism;

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

  public static class QueryRange {

    private final long startPointIncl; // always inclusive
    private final long endPoint; // inclusivity controlled by isEndPointExcl 
    private final boolean isEndPointExcl;

    public QueryRange(long startPointIncl, long endPoint, boolean isEndPointExcl) {
      this.startPointIncl = startPointIncl;
      this.endPoint = endPoint;
      this.isEndPointExcl = isEndPointExcl;
    }

    public long getStartPointIncl() {
      return startPointIncl;
    }

    public long getEndPoint() {
      return endPoint;
    }

    public boolean isEndPointExcl() {
      return isEndPointExcl;
    }

  }

  /**
   * Given a min, max and expected queryParallelism, generate all required queries that should be
   * executed.
   * @param min minimum value to filter splitColumn
   * @param max maximium value to filter splitColumn
   * @param parallelism max number of queries to generate
   * @param splitColumn the column that will be use to split and parallelize queries
   * @param queryBuilder template query builder
   * @return a list of SQL queries
   */
  protected static List<String> queriesForBounds(
         long min, long max, int parallelism, String splitColumn, QueryBuilder queryBuilder) {
    
    List<QueryRange> ranges = generateRanges(min, max, parallelism);

    return ranges.stream()
        .map(
            x ->
                queryBuilder
                    .copy() // we create a new query here
                    .withParallelizationCondition(
                        splitColumn, x.getStartPointIncl(), x.getEndPoint(), x.isEndPointExcl())
                    .build())
        .collect(Collectors.toList());
  }

  /**
   * Given a min, max and expected queryParallelism, generate all required queries that should be
   * executed.
   * @param min minimum value to filter splitColumn
   * @param max maximium value to filter splitColumn
   * @param parallelism max number of queries to generate
   * @return A list query ranges
   */
  protected static List<QueryRange> generateRanges(
          long min, long max, int parallelism) {
    // We try not to generate more than queryParallelism. Hence we don't want to loose number by
    // rounding down. Also when queryParallelism is higher than max - min, we don't want 0 ranges
    long bucketSize = (long) Math.ceil((double) (max - min) / (double) parallelism);
    bucketSize =
            bucketSize == 0 ? 1 : bucketSize; // If max and min is same, we export only 1 query
    List<QueryRange> ranges = new ArrayList<>(parallelism);

    long i = min;
    while (i + bucketSize < max) {
      // Include lower bound and exclude the upper bound.
      ranges.add(new QueryRange(i, i + bucketSize, true));
      i = i + bucketSize;
    }

    // Add last query
    if (i + bucketSize >= max) {
      // If bucket size exceeds max, we must use max and the predicate
      // should include upper bound.
      ranges.add(new QueryRange(i, max, false));
    }

    // If queryParallelism is higher than max-min, this will generate less ranges.
    // But lets never generate more ranges.
    checkState(ranges.size() <= parallelism,
            "Unable to generate expected number of ranges for given min max.");

    return ranges;
  }

  protected static List<QueryRange> generateRanges(Iterable<Long> bounds) {
    List<QueryRange> ranges = new ArrayList<>();
    long prev = 0;

    for (long b : bounds) {
      ranges.add(new QueryRange(prev, b, false));
    }

    return ranges;
  }


  protected static List<String> queriesForEvenDistribution(Iterable<Long> bounds,
                                                      String splitColumn,
                                                      QueryBuilder queryBuilder) {

    List<QueryRange> ranges = generateRanges(bounds);

    return ranges.stream()
      .map(
        x ->
          queryBuilder
            .copy() // we create a new query here
            .withParallelizationCondition(
              splitColumn, x.getStartPointIncl(), x.getEndPoint(), x.isEndPointExcl())
                        .build())
            .collect(Collectors.toList());

//    long prev = 0;
//
//    for (long b : bounds) {
//      String condition = " AND %1$s >= %2$s AND %1$s < %3$s ";
//
//      queries.add(String.format(queryFormat,
//              String.format(condition, splitColumn, prev, b)));
//
//      prev = b;
//    }
//
//    // add last partition
//    queries.add(String.format(queryFormat, String.format(" AND %s >= %s", splitColumn, prev)));
//
//    return queries;
  }




}
