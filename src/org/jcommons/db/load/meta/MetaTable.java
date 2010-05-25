package org.jcommons.db.load.meta;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;
import org.jcommons.db.column.MetaColumn;
import org.jcommons.db.column.MetaColumnUtils;
import org.jcommons.db.exception.TableNotFoundException;
import org.jcommons.lang.string.MessageUtils;

/**
 * Utility class to retrieve the meta data for a given table.
 *
 * @author Thorsten Goeckeler
 */
public final class MetaTable
{
  private static final String SELECT = "select * from ${table} where 1=0";

  /** hide sole constructor */
  private MetaTable() {
  }

  /**
   * Retrieve the column meta data for all columns of a given table
   *
   * @param dataSource the data source to use, never null
   * @param tableName the table name to query on, never null
   * @return the list of meta data on all columns, can be empty but never null
   * @throws TableNotFoundException if database cannot be accessed or privileges are missing
   */
  public static List<MetaColumn> getMetaData(final DataSource dataSource, final String tableName)
    throws TableNotFoundException
  {
    Map<String, String> parameter = new HashMap<String, String>();
    parameter.put("table", StringUtils.upperCase(tableName));
    String sql = MessageUtils.message(SELECT).with("table", tableName).toString();

    List<MetaColumn> columns = null;
    try {
      List<String> primaryKeys = primaryKeys(dataSource, tableName);

      QueryRunner query = new QueryRunner(dataSource);
      columns = query.query(sql, new MetaColumnHandler(primaryKeys));

    } catch (SQLException ex) {
      throw new TableNotFoundException(tableName, ex);
    }
    return columns;
  }

  /**
   * List all columns that form the primary key of the given table.
   *
   * @param dataSource the database connection to use
   * @param tableName the table for which we need the primary keys
   * @return the list of primary keys, can be empty but never null
   * @throws SQLException if the database cannot be accessed or the driver does not support this feature
   */
  private static List<String> primaryKeys(final DataSource dataSource, final String tableName)
    throws SQLException
  {
    List<String> keys = new LinkedList<String>();
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet primaries = meta.getPrimaryKeys(null, null, StringUtils.upperCase(tableName));

      while (primaries.next()) {
        keys.add(primaries.getString("COLUMN_NAME"));
      }
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return keys;
  }

  /**
   * Determines all tables that the given table depends upon.
   *
   * @param dataSource the database connection to use
   * @param tableName the table for which we want to know which tables this one depends upon
   * @return the list of table names that this table references, can be empty but never <code>null</code>
   * @throws SQLException if the database cannot be accessed or the driver does not support this feature
   */
  public static Set<String> dependsOn(final DataSource dataSource, final String tableName)
    throws SQLException
  {
    Set<String> tables = new HashSet<String>();
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet foreigns = meta.getImportedKeys(null, null, StringUtils.upperCase(tableName));

      while (foreigns.next()) {
        tables.add(foreigns.getString("PKTABLE_NAME"));
      }
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return tables;
  }

  /**
   * Determines all tables that the given table depends upon and requires data for.
   *
   * @param dataSource the database connection to use
   * @param tableName the table for which we want to know which tables this one depends upon
   * @return the list of table names that this table references on mandatory keys, never <code>null</code>
   * @throws SQLException if the database cannot be accessed or the driver does not support this feature
   */
  public static Set<String> dependsMandatoryOn(final DataSource dataSource, final String tableName)
    throws SQLException
  {
    List<MetaColumn> columns = getMetaData(dataSource, tableName);
    Set<String> tables = new HashSet<String>();
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet foreigns = meta.getImportedKeys(null, null, StringUtils.upperCase(tableName));
      while (foreigns.next()) {
        String foreignKey = foreigns.getString("FKCOLUMN_NAME");
        MetaColumn foreignColumn = MetaColumnUtils.findByColumnName(foreignKey, columns);

        if (foreignColumn != null && foreignColumn.isNotNullable()) {
          tables.add(foreigns.getString("PKTABLE_NAME"));
        }
      }
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return tables;
  }
}
