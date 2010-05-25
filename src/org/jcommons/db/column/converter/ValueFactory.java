package org.jcommons.db.column.converter;

import java.util.HashMap;
import java.util.Map;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.db.column.MetaType;
import org.jcommons.message.Message;

/**
 * Factory that converts the given value automatically into the object required by the given column.
 */
public final class ValueFactory
{
  private static Map<MetaType, ToValue<?>> converters = new HashMap<MetaType, ToValue<?>>();
  private static ToValue<Object> toNull = new ToNull();

  static {
    converters.put(MetaType.DATE, new ToDate());
    converters.put(MetaType.TIMESTAMP, new ToTimestamp());
    converters.put(MetaType.NUMBER, new ToNumber());
    converters.put(MetaType.STRING, new ToString());
  }

  /** hide sole constructor */
  private ValueFactory() {
  }

  /**
   * Converts a given string value into the respective object if possible.
   *
   * @param meta the meta column data
   * @param value the string value to convert
   * @param validation message to add errors, warnings and further details to
   * @return the corresponding object value, can be <code>null</code>
   */
  public static Object valueOf(final MetaColumn meta, final String value, final Message validation) {
    ToValue<?> converter = converters.get(meta.getMetaType());
    if (converter == null) converter = toNull;

    return converter.valueOf(meta, value, validation);
  }
}
