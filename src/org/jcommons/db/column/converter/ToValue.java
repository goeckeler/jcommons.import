package org.jcommons.db.column.converter;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.message.Message;

/**
 * internal value converter
 *
 * @param <T> the object type that is returned
 */
public interface ToValue<T>
{
  /**
   * Converts a given string value into the respective object if possible.
   *
   * @param meta the meta column data
   * @param value the string value to convert
   * @param validation message to add errors, warnings and further details to
   * @return the corresponding object value, can be <code>null</code>
   */
  T valueOf(MetaColumn meta, String value, Message validation);
}
