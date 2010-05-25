package org.jcommons.db.column.converter;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.lang.string.MessageUtils;
import org.jcommons.lang.string.StringUtils;
import org.jcommons.message.Fault;
import org.jcommons.message.Message;

/**
 * default value converter that handles mandatory and empty values
 *
 * @param <T> the object type that is returned
 */
public abstract class AbstractToValue<T>
  implements ToValue<T>
{
  private static final String REQUIRED = "Value for ${table}.${column} is required.";

  /** {@inheritDoc} */
  @Override
  public T valueOf(final MetaColumn meta, final String value, final Message validation) {
    if (StringUtils.isBlank(value)) {
      if (meta != null && meta.isNotNullable()) {
        MessageUtils fault = MessageUtils.message(REQUIRED).with("table", meta.getTable());
        fault.with("column", meta.getName());
        validation.add(new Fault(fault.toString()));
      }

      // if no meta data is available the value will be always null, in addition to null values
      return null;
    }

    return objectOf(meta, value.trim(), validation);
  }

  /**
   * Converts a given non-empty string value into the respective object if possible.
   *
   * @param meta the meta column data, never null
   * @param value the non-empty trimmed string value to convert
   * @param validation message to add errors, warnings and further details to, never null
   * @return the corresponding object value, can be <code>null</code>
   */
  protected abstract T objectOf(final MetaColumn meta, final String value, final Message validation);

  /**
   * Pre-populates a message object text with the standard parameters from the meta column.
   *
   * @param message the string with place holders to be used as the message
   * @param meta the current meta column, not null
   * @param value the current value of the object
   * @return the message utility class to be used to generate the validation message
   */
  protected MessageUtils message(final String message, final MetaColumn meta, final Object value) {
    MessageUtils text = MessageUtils.message(message).with("table", meta.getTable());
    text.with("column", meta.getName()).with("value", value).with("size", meta.getSize());
    text.with("precision", meta.getPrecision()).with("fraction", meta.getFraction());
    return text;
  }
}
