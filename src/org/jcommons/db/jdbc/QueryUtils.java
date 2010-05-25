package org.jcommons.db.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.jcommons.db.exception.TableNotFoundException;
import org.jcommons.lang.string.MessageUtils;


/**
 * Utility class to facilitate easy access to the database.
 *
 * @author Thorsten Goeckeler
 */
public final class QueryUtils
{
  private static final String COUNT = "select count(*) from ${table}";

  /** hide sole constructor */
  private QueryUtils() {
  }

  /**
   * Counts the rows of a given table.
   *
   * @param dataSource the data source to use, never null
   * @param tableName the table name to query on, never null
   * @return the number of rows in the table
   * @throws TableNotFoundException if database cannot be accessed or privileges are missing
   */
   public static int countRows(final DataSource dataSource, final String tableName)
    throws TableNotFoundException
  {
    String sql = MessageUtils.message(COUNT).with("table", tableName).toString();

    int count = 0;
    try {
      count = new QueryRunner(dataSource).query(sql, new IntegerHandler());
    } catch (SQLException ex) {
      throw new TableNotFoundException(tableName, ex);
    }
    return count;
  }

}
