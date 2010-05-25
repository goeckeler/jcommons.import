package org.jcommons.db.column.converter;

import java.util.Date;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.lang.time.DateUtils;
import org.jcommons.message.Message;
import org.jcommons.message.Warning;

/** tries to convert the current object into a date value */
public class ToDate
  extends AbstractToValue<Date>
{
  private static final String OUTDATED = "\"${value}\" is not a valid date for ${table}.${column} and will be ignored.";

  /** {@inheritDoc} */
  @Override
  protected Date objectOf(final MetaColumn meta, final String value, final Message validation) {
    if (meta.isDate()) {
      Date date = DateUtils.toTime(value);
      if (date == null) date = DateUtils.toDay(value);

      if (date == null) {
        validation.add(new Warning(message(OUTDATED, meta, value).toString()));
      } else {
        return date;
      }
    }
    return null;
  }
}
