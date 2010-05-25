package org.jcommons.db.column.converter;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.message.Message;
import org.jcommons.message.Warning;

/** tries to convert the current object into a string value */
public class ToString
  extends AbstractToValue<String>
{
  private static final String TRUNCATE =
    "Value \"${value}\" is too large for ${table}.${column}"
        + " and will be truncated from ${length} to ${size} characters.";

  /** {@inheritDoc} */
  @Override
  protected String objectOf(final MetaColumn meta, final String value, final Message validation) {
    if (value.length() > meta.getSize()) {
      validation.add(new Warning(message(TRUNCATE, meta, value).with("length", value.length()).toString()));
      return value.substring(0, meta.getSize());
    }
    return value;
  }
}
