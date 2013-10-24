package org.jcommons.db.column.converter;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.message.Message;
import org.jcommons.message.Warning;

/** empty converter that will not convert the value at all as no converter is available */
public class ToNull
  extends AbstractToValue<Object>
{
  private static final String INVALID =
      "Cannot import \"${value}\" into ${table}.${column} as I don't know how to do it.";

  /** {@inheritDoc} */
  @Override
  protected Object objectOf(final MetaColumn meta, final String value, final Message validation) {
    validation.add(new Warning(message(INVALID, meta, value).toString()));
    return null;
  }
}
