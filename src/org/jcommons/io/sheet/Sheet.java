package org.jcommons.io.sheet;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.jcommons.db.column.MetaColumn;
import org.jcommons.io.text.Table;
import org.jcommons.lang.string.MessageUtils;
import org.jcommons.message.*;


/**
 * A sheet names a table and provides means to convert the string data into typed data
 *
 * Think of it as an Excel tab in an Excel file that contains tabular data only. As the table is currently read-only,
 * the sheet is currently read-only.
 *
 * @author Thorsten Goeckeler
 */
public class Sheet
{
  private String name;
  private Table table;
  private List<MetaColumn> columns;
  private Map<String, MetaColumn> columnMap;

  /** @return the name of this sheet, never null */
  public String getName() {
    return StringUtils.defaultString(name);
  }

  /**
   * Define a sensible name to this sheet, should be either the file name or the tab name
   *
   * @param name the name to identify this sheet by
   * @return this to allow chaining
   */
  public Sheet setName(final String name) {
    this.name = name;
    return this;
  }

  /** @return the underlying table of this sheet */
  public Table getTable() {
    return table;
  }

  /**
   * Assign the given table to this sheet
   *
   * @param table the tabular data that this sheet references
   * @return this to allow chaining
   */
  public Sheet setTable(final Table table) {
    this.table = table;
    return this;
  }

  /**
   * Provides the typed column headers from the database to be matched with the given table
   *
   * @param columns the database column definitions
   * @return this to allow chaining
   */
  public Sheet setColumns(final List<MetaColumn> columns) {
    this.columns = columns;
    return this;
  }

  /** @return the currently known database meta data for the table associated with this sheet */
  public List<MetaColumn> getColumns() {
    if (this.columns == null) return Collections.emptyList();
    return this.columns;
  }

  private static final String COLUMN_REQUIRED = "Table \"${table}\" requires values for column \"${column}\".";
  private static final String COLUMN_MISSING = "Table \"${table}\" has no column \"${column}\".";

  private String error(final String message, final String table, final String column) {
    return MessageUtils.message(message).with("table", table).with("column", column).toString();
  }

  /**
   * Find the meta column by a given name
   *
   * @param columnName the case insensitive column name
   * @return the respective column or <code>null</code> if there is no such column
   */
  private MetaColumn getColumn(final String columnName) {
    if (StringUtils.isBlank(columnName)) return null;

    for (MetaColumn column : getColumns()) {
      if (columnName.equalsIgnoreCase(column.getName())) {
        return column;
      }
    }

    return null;
  }

  /**
   * Validates that given database columns can be matched to the table columns.
   *
   * @return the list of error messages, never <code>null</code>
   */
  public Message validateColumns() {
    Messages errors = new Messages();

    if (getColumns().isEmpty() || getTable() == null || getTable().getColumns().isEmpty()) return errors;

    for (MetaColumn column : getColumns()) {
      if (column.isNotNullable() && getTable().indexOf(column.getName()) < 0) {
        errors.add(new Fault(error(COLUMN_REQUIRED, getName(), column.getName())));
      }
    }

    for (String column : getTable().getColumns()) {
      if (getColumn(column) == null) {
        errors.add(new Fault(error(COLUMN_MISSING, getName(), column)));
      }
    }
    return errors;
  }
}
