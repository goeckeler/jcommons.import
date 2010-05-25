package org.jcommons.db.load.meta;

import java.sql.*;
import java.util.*;

import org.apache.commons.dbutils.ResultSetHandler;
import org.jcommons.db.column.MetaColumn;

/**
 * Creates the meta data details on all columns of a given query.
 *
 * @author Thorsten Goeckeler
 */
class MetaColumnHandler
  implements ResultSetHandler<List<MetaColumn>>
{
  private List<String> primaryKeys;

  /** Handler with no known primary keys. */
  public MetaColumnHandler() {
    this(null);
  }

  /**
   * Handler with known primary keys.
   *
   * @param primaryKeys the column names of the primary key for the result set
   */
  public MetaColumnHandler(final List<String> primaryKeys) {
    if (primaryKeys == null) {
      this.primaryKeys = Collections.emptyList();
    } else {
      this.primaryKeys = primaryKeys;
    }
  }

  /**
   * Retrieve the meta data only on a given result set
   *
   * @param rs the current result set of a database query
   * @return the meta data on the columns contained in the query
   * @throws SQLException if the database cannot be accessed
   */
  public List<MetaColumn> handle(final ResultSet rs)
    throws SQLException
  {
    ResultSetMetaData meta = rs.getMetaData();
    int columns = meta.getColumnCount();
    List<MetaColumn> list = new ArrayList<MetaColumn>(Math.max(2, columns));

    for (int i = 1; i <= columns; ++i) {
      MetaColumn column = new MetaColumn();

      column.setName(meta.getColumnName(i));
      column.setLabel(meta.getColumnLabel(i));
      column.setTable(meta.getTableName(i));
      column.setSchema(meta.getSchemaName(i));
      column.setSize(meta.getColumnDisplaySize(i));
      column.setPrecision(meta.getPrecision(i));
      column.setFraction(meta.getScale(i));
      column.setNullable(ResultSetMetaData.columnNullable == meta.isNullable(i));
      column.setType(meta.getColumnClassName(i));

      if (primaryKeys.contains(column.getName())) {
        column.setPrimary(true);
      }

      list.add(column);
    }

    return list;
  }
}
