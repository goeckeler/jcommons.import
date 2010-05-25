package org.jcommons.db.jdbc;

import java.sql.*;

import org.apache.commons.dbutils.ResultSetHandler;

/**
 * Extracts a single integer from the given query.
 *
 * Useful for count(*), max(), min(), and so on.
 *
 * @author Thorsten Goeckeler
 */
class IntegerHandler
  implements ResultSetHandler<Integer>
{
  /**
   * Retrieve the first column as an integer.
   *
   * @param rs the current result set of a database query
   * @return the first column as an integer of the query
   * @throws SQLException if the database cannot be accessed
   */
  public Integer handle(final ResultSet rs)
    throws SQLException
  {
    if (!rs.next()) return 0;

    ResultSetMetaData meta = rs.getMetaData();
    if (meta.getColumnCount() != 1) return 0;

    return rs.getInt(1);
  }
}
