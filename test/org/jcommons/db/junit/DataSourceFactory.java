package org.jcommons.db.junit;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * Utility class for JUnit tests that instantiates ready-made data sources
 *
 * @author Thorsten Goeckeler
 */
public final class DataSourceFactory
{
  private static DataSource dataSource;

  /** hide sole constructor */
  private DataSourceFactory() {
  }

  /** @return a reference to the default in-memory database */
  public static DataSource createMemoryDataSource() {
    if (dataSource == null) {
      BasicDataSource basicDataSource = new BasicDataSource();
      basicDataSource.setDriverClassName("org.hsqldb.jdbcDriver");
      basicDataSource.setUrl("jdbc:hsqldb:mem:junit");
      basicDataSource.setUsername("sa");
      basicDataSource.setPassword("");
      dataSource = basicDataSource;
    }
    return dataSource;
  }
}
