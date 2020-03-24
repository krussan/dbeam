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

import com.spotify.dbeam.dialects.SqlDialect;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for raw SQL query.
 */
public class QueryBuilder implements Serializable {

  private static final char SQL_STATEMENT_TERMINATOR = ';';
  private static final String DEFAULT_SELECT_CLAUSE = "SELECT *";
  private static final String DEFAULT_WHERE_CLAUSE = "WHERE 1=1";

  private static Logger LOGGER = LoggerFactory.getLogger(QueryBuilder.class);

  interface QueryBase {

    String getBaseSql();

    String getTableName();

    QueryBase copyWithSelect(final String selectClause);
  }

  /**
   * Represents table-based query, which we have full control of.
   * 
   * <p>Immutable entity. 
   */
  private static class TableQueryBase implements QueryBase {

    private final String tableName;
    private final String selectClause;

    public TableQueryBase(final String tableName) {
      this(tableName, DEFAULT_SELECT_CLAUSE);
    }

    public TableQueryBase(final String tableName, final String selectClause) {
      this.tableName = tableName;
      this.selectClause = selectClause;
    }

    @Override
    public String getBaseSql() {
      return String.format("%s FROM %s %s",
              selectClause, tableName, DEFAULT_WHERE_CLAUSE);
    }

    @Override
    public String getTableName() {
      return tableName;
    }

    @Override
    public TableQueryBase copyWithSelect(final String selectClause) {
      return new TableQueryBase(this.tableName, selectClause);
    }

    @Override
    public int hashCode() {
      return tableName.hashCode();
    }
  }

  /**
   * Represents user-provided raw query, which we have no control of.
   *
   * <p>Immutable entity. 
   */
  private static class UserQueryBase implements QueryBase {

    private final String userSqlQuery;
    private final String selectClause;

    public UserQueryBase(final String userSqlQuery) {
      this(userSqlQuery, DEFAULT_SELECT_CLAUSE);
    }

    public UserQueryBase(final String userSqlQuery, final String selectClause) {
      this.userSqlQuery = removeTrailingSymbols(userSqlQuery);
      this.selectClause = selectClause;
    }

    @Override
    public String getBaseSql() {
      return String.format("%s FROM (%s) as user_sql_query %s",
              selectClause, userSqlQuery, DEFAULT_WHERE_CLAUSE);
    }

    @Override
    public String getTableName() {
      return "user_sql_query";
    }

    @Override
    public UserQueryBase copyWithSelect(String selectClause) {
      return new UserQueryBase(this.userSqlQuery, selectClause);
    }

    @Override
    public int hashCode() {
      return userSqlQuery.hashCode();
    }
  }

  private final QueryBase base;
  private final List<String> whereConditions = new LinkedList<>();
  private Optional<String> limitStr = Optional.empty();
  private Optional<String> groupByStr = Optional.empty();
  private Optional<String> orderByStr = Optional.empty();
  private SqlDialect dialect;
  
  private QueryBuilder(final QueryBase base, final SqlDialect dialect) {
    this.base = base;
    this.dialect = dialect;
  }

  private QueryBuilder(final QueryBase base, final QueryBuilder that) {
    this.base = base;
    this.whereConditions.addAll(that.whereConditions);
    this.limitStr = that.limitStr;
    this.groupByStr = that.groupByStr;
    this.orderByStr = that.orderByStr;
    this.dialect = that.dialect;
  }

  private QueryBuilder(final QueryBuilder that) {
    this.base = that.base;
    this.whereConditions.addAll(that.whereConditions);
    this.limitStr = that.limitStr;
    this.groupByStr = that.groupByStr;
    this.orderByStr = that.orderByStr;
    this.dialect = that.dialect;
  }

  public QueryBuilder copy() {
    return new QueryBuilder(this);
  }

  public static QueryBuilder fromTablename(final String tableName, final SqlDialect dialect) {
    return new QueryBuilder(new TableQueryBase(tableName), dialect);
  }

  public static QueryBuilder fromSqlQuery(final String sqlQuery, final SqlDialect dialect) {
    return new QueryBuilder(new UserQueryBase(sqlQuery), dialect);
  }

  public QueryBuilder withPartitionCondition(
          String partitionColumn, String startPointIncl, String endPointExcl) {
    whereConditions.add(createSqlPartitionCondition(partitionColumn, startPointIncl, endPointExcl));
    return this;
  }
          
  private static String createSqlPartitionCondition(
      String partitionColumn, String startPointIncl, String endPointExcl) {
    return String.format(
        " AND %s >= '%s' AND %s < '%s'",
        partitionColumn, startPointIncl, partitionColumn, endPointExcl);
  }

  public QueryBuilder withParallelizationCondition(
      String partitionColumn, long startPointIncl, long endPoint, boolean isEndPointExcl) {
    whereConditions.add(
        createSqlSplitCondition(partitionColumn, startPointIncl, endPoint, isEndPointExcl));
    return this;
  }

  private static String createSqlSplitCondition(
      String partitionColumn, long startPointIncl, long endPoint, boolean isEndPointExcl) {

    String upperBoundOperation = isEndPointExcl ? "<" : "<=";
    return String.format(
        " AND %s >= %s AND %s %s %s",
        partitionColumn, startPointIncl, partitionColumn, upperBoundOperation, endPoint);
  }

  /**
   * Returns generated SQL query string.
   * 
   * @return generated SQL query string.
   */
  public String build() {
    String initial = replaceInitialSelect(base.getBaseSql());
    StringBuilder buffer = new StringBuilder(initial);
    whereConditions.forEach(buffer::append);

    if (this.dialect.getTopLimitPosition() == SqlDialect.LimitPosition.LAST) {
      limitStr.ifPresent(buffer::append);
    }

    groupByStr.ifPresent(buffer::append);
    orderByStr.ifPresent(buffer::append);

    return buffer.toString();
  }

  private String replaceInitialSelect(String initial) {
    if (limitStr.isPresent()
        && this.dialect.getTopLimitPosition() == SqlDialect.LimitPosition.AFTER_SELECT) {
      Pattern p = Pattern.compile("^SELECT\\s*", Pattern.CASE_INSENSITIVE);
      Matcher m = p.matcher(initial);
      String result = m.replaceFirst(String.format("SELECT %s ", limitStr.get()));
      LOGGER.info("Replaced limit :: {}", result);

      return result;
    } else {
      return initial;
    }
  }

  public String getTableName() {
    return base.getTableName();
  }

  private static String removeTrailingSymbols(String sqlQuery) {
    // semicolon followed by any number of delimiters at the end of the string
    String regex = String.format("%c([\\s]*)$", SQL_STATEMENT_TERMINATOR);
    return sqlQuery.replaceAll(regex, "$1");
  }

  public QueryBuilder withLimit(long limit) {
    limitStr = Optional.of(String.format(" %s %d", dialect.getLimitKeyword(), limit));
    return this;
  }

  public QueryBuilder withLimitOne() {
    return withLimit(1L);
  }

  public QueryBuilder withGroupBy(String groupBy) {
    groupByStr = Optional.of(String.format(" GROUP BY %s", groupBy));
    return this;
  }

  public QueryBuilder withOrderBy(String orderBy) {
    orderByStr = Optional.of(String.format(" ORDER BY %s", orderBy));
    return this;
  }

  @Override
  public String toString() {
    return build();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof QueryBuilder) {
      QueryBuilder that = (QueryBuilder) obj;
      return build().equals((that.build()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return base.hashCode();
  }

  /**
   * Generates a new query to get MIN/MAX values for splitColumn.  
   * 
   * @param splitColumn column to use
   * @param minSplitColumnName MIN() column value alias
   * @param maxSplitColumnName MAX() column value alias
   * @return a new query builder
   */
  public QueryBuilder generateQueryToGetLimitsOfSplitColumn(
      String splitColumn,
      String minSplitColumnName,
      String maxSplitColumnName) {

    String selectMinMax = String.format(
            "SELECT MIN(%s) as %s, MAX(%s) as %s",
            splitColumn,
            minSplitColumnName,
            splitColumn,
            maxSplitColumnName);
    
    return new QueryBuilder(base.copyWithSelect(selectMinMax), this);
  }

  public QueryBuilder generateQueryToDistinctDistributionBounds(
          String splitColumn) {

    String select = String.format("SELECT DISTINCT %s", splitColumn);

    return new QueryBuilder(base.copyWithSelect(select), this);
  }

  public QueryBuilder generateDistributionQuery(String splitColumn) {
    String select = String.format("SELECT MAX(%s), COUNT(1)", splitColumn);

    return new QueryBuilder(base.copyWithSelect(select), this);
  }

  public QueryBuilder generateDistributionBucketQuery(String splitColumn) {
    String select = String.format(
            "SELECT %s, COUNT(1) AS cnt",
            splitColumn);

    return new QueryBuilder(base.copyWithSelect(select), this.dialect)
            .withGroupBy(splitColumn)
            .withOrderBy(splitColumn);
  }

}
