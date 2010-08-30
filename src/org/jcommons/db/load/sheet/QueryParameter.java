package org.jcommons.db.load.sheet;

import static org.apache.commons.lang.StringUtils.defaultString;

import java.util.*;

import org.jcommons.db.column.Column;
import org.jcommons.db.column.MetaColumn;
import org.jcommons.io.sheet.Sheet;

/**
 * Maps meta-columns and columns to their respective index in any given query.
 *
 * @author Thorsten Goeckeler
 */
public class QueryParameter
{
  private Map<String, Set<Integer>> indices;
  private Map<String, MetaColumn> columns;

  /**
   * Add a mapping from meta column to index.
   *
   * @param column the meta column we know a new index of, never <code>null</code>
   * @param index the JDBC parameter index where to replace the ? with the respective value
   */
  public void add(final MetaColumn column, final Integer index) {
    String key = key(column);

    // add look-up for meta-columns
    MetaColumn meta = columns.get(key);
    if (meta == null) columns.put(key, meta);

    // add index to existing indices
    Set<Integer> positions = indices.get(key);
    if (positions == null) {
      positions = new HashSet<Integer>();
      indices.put(key, positions);
    }
    positions.add(index);
  }

  /**
   * Converts the given row of the sheet into an object array of query parameters.
   *
   * @param sheet the corresponding sheet for this query parameter
   * @param row the row index to be converted into an object array
   * @return the object array with the respective copy of date from the table
   */
  public Object[] row(final Sheet sheet, final int row) {
    Object[] data = new Object[size()];
    for (String columnName : sheet.getTable().getColumns()) {
      Set<Integer> positions = indices.get(columnName.toLowerCase());
      if (positions != null && !positions.isEmpty()) {
        Column column = new Column();
        column.setMeta(columns.get(columnName.toLowerCase()));
        column.setValue(sheet.getTable().getValue(columnName, row));

        for (Integer position : positions) {
          // TODO : Log error messages
          data[position] = column.getObject();
        }
      }
    }

    return data;
  }

  public Object[][] rows(final Sheet sheet) {
    // TODO : convert complete sheet to object array
    return null;
  }

  /**
   * Create a unique key for column using the table name and the attribute name.
   *
   * @param column the column we want to look-up later
   * @return a tableName.columnName key for this column
   */
  private String key(final MetaColumn column) {
    return defaultString(column.getName()).toLowerCase();
  }

  /**
   * Calculates the size of a row for the given indices.
   *
   * @return the size for an object row
   */
  private int size() {
    int size = 0;
    for (Set<Integer> set : indices.values()) {
      size += set.size();
    }
    return size;
  }
}
