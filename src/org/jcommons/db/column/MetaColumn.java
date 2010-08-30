package org.jcommons.db.column;

import static org.jcommons.lang.string.StringBuilderUtils.appendIfNotEmpty;

import org.apache.commons.lang.StringUtils;

/**
 * Contains the database meta data for a single column
 *
 * @author Thorsten Goeckeler
 */
public class MetaColumn
{
  private String name;
  private String type;
  private MetaType metaType;
  private Integer size;
  private Integer precision;
  private Integer fraction;

  private boolean nullable;
  private boolean primary = false;

  private String label;
  private String schema;
  private String table;

  /** @return the column name */
  public String getName() {
    return name;
  }

  /**
   * Set the name of the column.
   *
   * @param name the name to set
   */
  public void setName(final String name) {
    this.name = name;
  }

  /** @return the class name of the respective Java type */
  public String getType() {
    return type;
  }

  /** @return the simple name of the class of the respective Java type for debugging purposes */
  public String getSimpleType() {
    if (StringUtils.isBlank(getType())) return "ANY";
    return getType().substring(Math.max(0, 1 + getType().lastIndexOf('.')));
  }

  /** @return the meta type in which we can convert this column, <code>null</code> if no type can be determined */
  public MetaType getMetaType() {
    // not determined yet? can only change if the type is changed
    if (metaType == null) {
      if (isTimestamp()) {
        return MetaType.TIMESTAMP;
      } else if (isDate()) {
        return MetaType.DATE;
      } else if (isNumeric()) {
        return MetaType.NUMBER;
      } else if (!"ANY".equalsIgnoreCase(getSimpleType())) { return MetaType.STRING; }
    }

    return null;
  }

  /**
   * Set the Java type class name.
   *
   * @param type the type to set
   */
  public void setType(final String type) {
    this.type = type;
    this.metaType = null;
  }

  /*** @return the size of the column, this is the string length for numeric values */
  public Integer getSize() {
    return size;
  }

  /**
   * Define the size of this column.
   *
   * @param size the maximum size of the column
   */
  public void setSize(final Integer size) {
    this.size = size;
  }

  /** @return the amount of digits of the integral part if it is a numeric value, mostly 0 */
  public Integer getPrecision() {
    return precision;
  }

  /**
   * Define the integral part of a numeric value.
   *
   * @param precision the amount of digits that form the integral part without the mantissa
   */
  public void setPrecision(final Integer precision) {
    this.precision = precision;
  }

  /** @return the amount of digits of the fraction part if it is a numeric value, that is the mantissa, mostly 0 */
  public Integer getFraction() {
    return fraction;
  }

  /**
   * Define the fraction part of a numeric value.
   *
   * @param fraction the amount of digits that form the fraction (the mantissa)
   */
  public void setFraction(final Integer fraction) {
    this.fraction = fraction;
  }

  /** @return true if this column can contain <code>null</code> values, otherwise <code>false</code> */
  public boolean isNullable() {
    return nullable;
  }

  /** @return true if this column can not contain <code>null</code> values, otherwise <code>false</code> */
  public boolean isNotNullable() {
    return !isNullable();
  }

  /**
   * Declare whether this column can contain <code>null</code> values or not.
   *
   * @param nullable <code>true</code> if <code>null</code> values are allowed, <code>false</code> otherwise
   */
  public void setNullable(final boolean nullable) {
    this.nullable = nullable;
  }

  /** @return true if this column is part of the primary key */
  public boolean isPrimary() {
    return primary;
  }

  /**
   * Declare whether this column is part of the primary key.
   *
   * @param primary <code>true</code> if this is a primary key part, <code>false</code> otherwise
   */
  public void setPrimary(final boolean primary) {
    this.primary = primary;
  }

  /** @return the label of the column given in as the alternative name in the SQL statement */
  public String getLabel() {
    return label;
  }

  /**
   * Indicate which label was used for this column.
   *
   * @param label the label name for this column to give it an alternative name
   */
  public void setLabel(final String label) {
    this.label = label;
  }

  /** @return the schema name for this table */
  public String getSchema() {
    return schema;
  }

  /**
   * Indicate which schema is addressed by the table of this column.
   *
   * @param schema the schema name of the table of this column
   */
  public void setSchema(final String schema) {
    this.schema = schema;
  }

  /** @return the table name which contains this column */
  public String getTable() {
    return table;
  }

  /**
   * Set the table name for this column
   *
   * @param table the table name that contains this column
   */
  public void setTable(final String table) {
    this.table = table;
  }

  /** @return true if this column can contain date details, can be day, time or time stamp */
  public boolean isDate() {
    String type = getSimpleType();
    return "date".equalsIgnoreCase(type) || "timestamp".equalsIgnoreCase(type);
  }

  /** @return true if this column contains time stamps */
  public boolean isTimestamp() {
    String type = getSimpleType();
    return "timestamp".equalsIgnoreCase(type);
  }

  /** @return true if the column can contain numeric data */
  public boolean isNumeric() {
    if (StringUtils.isNotBlank(getType())) {
      try {
        Class< ? > clazz = Class.forName(getType());
        return Number.class.isAssignableFrom(clazz);
      } catch (ClassNotFoundException e) {
        // unsupported type ...
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    StringBuilder text = new StringBuilder(StringUtils.defaultString(getTable()));
    text.append(".").append(StringUtils.defaultString(getName()));
    return text.toString().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object other) {
    if (this == other) return true;
    if (other == null) return false;
    if (!this.getClass().isAssignableFrom(other.getClass())) return false;

    MetaColumn that = (MetaColumn) other;
    return StringUtils.equals(this.toString(), that.toString());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    StringBuilder text = new StringBuilder();

    appendIfNotEmpty(text, null, getSchema(), ".");

    if (StringUtils.isNotBlank(getTable())) {
      text.append(getTable()).append('.');
    }

    text.append(StringUtils.defaultIfEmpty(getName(), "<column>")).append('[');
    text.append(getSimpleType());

    if (isNumeric()) {
      text.append("(").append(getPrecision());
      if (getFraction() != null && getFraction() > 0) {
        text.append(",").append(getFraction());
      }
      text.append(")");
    } else if (!isDate() && getSize() != null && getSize() > 0) {
      text.append("(").append(getSize()).append(")");
    }
    text.append(']');

    text.append(isPrimary() ? "!" : (isNullable() ? "" : "*"));
    return text.toString();
  }
}
