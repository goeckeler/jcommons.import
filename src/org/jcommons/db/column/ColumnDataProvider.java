package org.jcommons.db.column;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.jcommons.io.data.DataProvider;
import org.jcommons.lang.string.NamedString;
import org.jcommons.message.*;

/**
 * Data provider that converts plain data using the meta column data from the database itself.
 *
 * @author Thorsten Goeckeler
 */
public class ColumnDataProvider
  implements DataProvider
{
  private List<MetaColumn> metaColumns;
  private Column[] columns;
  private String tableName;

  private String[] headers;
  private String[] values;
  private final Map<String, Integer> indices;

  private final Message validations;

  private static final String COLUMN_REQUIRED = "Table \"${table}\" requires values for column \"${column}\".";
  private static final String COLUMN_MISSING = "Table \"${table}\" has no column \"${column}\".";

  /** default constructor */
  public ColumnDataProvider() {
    metaColumns = null;
    columns = null;
    tableName = null;

    headers = null;
    indices = new HashMap<String, Integer>();
    values = new String[0];
    validations = new Messages();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    // just remove the current values but not the data structure
    Arrays.fill(values, null);
    validations.clear();
  }

  private String error(final String message, final String table, final String column) {
    return NamedString.message(message).with("table", table).with("column", column).toString();
  }

  /** {@inheritDoc} */
  @Override
  public String[] getHeaders() {
    return headers == null ? new String[0] : headers;
  }

  /**
   * Find the meta column by a given name.
   *
   * @param columnName the case insensitive column name
   * @return the respective column or <code>null</code> if there is no such column
   */
  public MetaColumn getMetaColumn(final String columnName) {
    if (StringUtils.isBlank(columnName)) return null;

    for (MetaColumn column : getMetaColumns()) {
      if (columnName.equalsIgnoreCase(column.getName())) { return column; }
    }

    return null;
  }

  /** @return the currently known database meta data for the table associated with this data provider */
  public List<MetaColumn> getMetaColumns() {
    if (this.metaColumns == null) return Collections.emptyList();
    return this.metaColumns;
  }

  /** @return the current table name, never <code>null</code> */
  public String getTable() {
    return StringUtils.defaultIfEmpty(tableName, "unknown");
  }

  /** {@inheritDoc} */
  @Override
  public Object getValue(final String column) {
    if (column == null) return null;
    for (int index = 0; index < getHeaders().length; ++index) {
      if (column.equalsIgnoreCase(getHeaders()[index])) { return getValueAt(index); }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Object getValueAt(final int index) {
    validate();
    Column[] columns = getColumns();
    if (index >= 0 && index < columns.length && values != null && index < values.length) {
      Column column = columns[index];
      column.setValue(values[index]);
      Object object = column.getObject();
      validations.add(column.validate());
      return object;
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Object[] getValues() {
    // TODO Auto-generated method stub
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void setHeaders(final String[] columns) {
    this.headers = columns;
    this.columns = null;
    validations.clear();

    // re-index the headers
    indices.clear();
    if (columns != null) {
      for (int index = 0; index < columns.length; ++index) {
        indices.put(this.headers[index], index);
      }
      this.values = new String[columns.length];
    }
  }

  /**
   * Provides the typed column headers from the database to be matched with the given table
   *
   * @param metaColumns the database column definitions
   * @return this to allow chaining
   */
  public ColumnDataProvider setMetaColumns(final List<MetaColumn> metaColumns) {
    this.metaColumns = metaColumns;
    this.columns = null;
    validations.clear();
    return this;
  }

  /**
   * Define the name of the table for which this data conversion takes place.
   *
   * @param tableName the name of the table that contains all columns, never <code>null</code>
   */
  public void setTable(final String tableName) {
    this.tableName = tableName;
    validations.clear();
  }

  /** {@inheritDoc} */
  @Override
  public void setValue(final String column, final String value) {
    Integer index = indices.get(column);
    if (index != null) setValueAt(index, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setValueAt(final int index, final String value) {
    validations.clear();
    if (index >= 0 || index < values.length) values[index] = value;
  }

  /** {@inheritDoc} */
  @Override
  public void setValues(final List<String> values) {
    validations.clear();
    Arrays.fill(this.values, null);

    if (values != null) {
      for (int index = 0; index < Math.min(values.size(), this.values.length); ++index) {
        this.values[index] = values.get(index);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setValues(final Map<String, String> values) {
    validations.clear();
    Arrays.fill(this.values, null);

    for (Map.Entry<String, String> item : values.entrySet()) {
      this.setValue(item.getKey(), item.getValue());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setValues(final String[] values) {
    validations.clear();
    if (values == null) {
      clear();
    } else {
      if (this.values.length == values.length) {
        this.values = values;
      } else {
        for (int index = 0; index < this.values.length; ++index) {
          this.values[index] = values[index];
        }
      }
    }
  }

  /**
   * Validates if the columns can be converted at all that is if the table columns can be mapped to the data columns.
   *
   * @return the list of error messages, should be empty to start conversion
   */
  public Message validateTable() {
    Messages errors = new Messages();

    if (getMetaColumns().isEmpty() || getTable() == null || headers == null) return errors;

    for (MetaColumn column : getMetaColumns()) {
      if (column.isNotNullable() && !existsHeader(column.getName())) {
        errors.add(new Fault(error(COLUMN_REQUIRED, getTable(), column.getName())));
      }
    }

    for (String column : getHeaders()) {
      if (getMetaColumn(column) == null) {
        errors.add(new Fault(error(COLUMN_MISSING, getTable(), column)));
      }
    }

    return errors;
  }

  /**
   * Validates if the data or which data could be converted.
   *
   * @return the list of error messages, should contain no faults for a successful conversion
   */
  public Message validate() {
    if (validations.isEmpty()) {
      // the data is validated on the fly during conversion, so we need to fill this data only if no validations has
      // been performed in the meantime
      validations.add(validateTable());
    }

    // only return the validations for the current moment
    return validations;
  }

  /** @return current conversion columns for the current columns and meta-data, never <code>null</code> */
  private Column[] getColumns() {
    if (columns == null || columns.length == 0) {
      if (getHeaders().length > 0 && !getMetaColumns().isEmpty() && getHeaders().length == getMetaColumns().size()) {
        columns = new Column[getHeaders().length];
        for (int index = 0; index < getHeaders().length; ++index) {
          Column column = new Column();
          column.setMeta(getMetaColumn(getHeaders()[index]));
        }
      } else {
        columns = new Column[0];
      }
    }

    return columns;
  }

  /**
   * Checks whether the given column name is a provided in the headers to be loaded.
   *
   * @param header the column name that should be present in data set we want to load
   * @return true if the header is provided, otherwise false
   */
  private boolean existsHeader(final String header) {
    return indexOfHeader(header) >= 0;
  }

  /**
   * Determines the index of the given column header in the data set we want to load.
   *
   * @param header the column name that should be present in data set we want to load
   * @return the corresponding index in the header array or -1 if the header is unknown
   */
  private int indexOfHeader(final String header) {
    for (int index = 0; index < getHeaders().length; ++index) {
      if (getHeaders()[index].equalsIgnoreCase(header)) {
        return index;
      }
    }
    return -1;
  }
}
