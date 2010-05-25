package org.jcommons.db.column;

import org.apache.commons.lang.StringUtils;
import org.jcommons.db.column.converter.ValueFactory;
import org.jcommons.message.Message;
import org.jcommons.message.Messages;

/**
 * Abstract notion of a database column value.
 *
 * A column is a data holder that holds the data value for a certain column and knows where this data comes from or
 * shall be stored to. In other words it holds the value for a certain column of a certain row for a given table, e.g.:
 *
 * <pre>
 * customer.name = &quot;x-root&quot;
 * </pre>
 *
 * @author Thorsten Goeckeler
 */
public class Column
{
  private MetaColumn meta;
  private String value;
  private Message message = null;

  /** @return the meta column describing this column */
  public MetaColumn getMeta() {
    return meta;
  }

  /** @return the current value of this column in plain text */
  public String getValue() {
    return value;
  }

  /**
   * Define the meta column for this column value.
   *
   * @param metaColumn the meta column that describes this column
   * @return this to allow chaining
   */
  public Column setMeta(final MetaColumn metaColumn) {
    this.meta = metaColumn;
    message = null;
    return this;
  }

  /**
   * Set the current value of this column as plain text.
   *
   * @param value the current value for this column, can be <code>null</code>
   * @return this to allow chaining
   */
  public Column setValue(final String value) {
    this.value = value;
    message = null;
    return this;
  }

  /** @return the current value as an object in the type of the column, null if the value is not given or invalid */
  public Object getObject() {
    // every time we do a conversion we clear the messages so we get the latest validation errors
    if (message == null) {
      message = new Messages();
    } else {
      message.clear();
    }

    return ValueFactory.valueOf(getMeta(), getValue(), message);
  }

  /**
   * Check if the value can be successfully converted.
   *
   * @return true if the current value can be successfully converted in a value that the database likes or is
   *         <code>null</code>, otherwise false
   */
  public boolean isValid() {
    Message validations = validate();
    return !(validations.isError() || validations.isWarning());
  }

  /** @return true if the value is empty or the column is unknown */
  public boolean isEmpty() {
    // blank values are regarded as empty as well
    // well, if we don't know in what to convert we also say it is an empty value
    return StringUtils.isBlank(getValue()) || getMeta() == null;
  }

  /** @return the validation messages for this object */
  public Message validate() {
    if (message == null) {
      getObject();
    }

    return message;
  }
}
