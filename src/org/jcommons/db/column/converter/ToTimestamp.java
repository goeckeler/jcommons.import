package org.jcommons.db.column.converter;

import java.sql.Timestamp;
import java.util.Date;

import org.jcommons.db.column.MetaColumn;
import org.jcommons.lang.time.DateUtils;
import org.jcommons.message.Message;
import org.jcommons.message.Warning;

/** tries to convert the current object into a time stamp value */
public class ToTimestamp
  extends AbstractToValue<Timestamp>
{
  private static final String OUTTIMED =
      "\"${value}\" is not a valid time stamp for ${table}.${column} and will be ignored.";

  /** {@inheritDoc} */
  @Override
  protected Timestamp objectOf(final MetaColumn meta, final String value, final Message validation) {
    if (meta.isTimestamp()) {
      Date date = DateUtils.toTime(value);
      if (date == null) {
        validation.add(new Warning(message(OUTTIMED, meta, value).toString()));
      } else {
        return new Timestamp(date.getTime());
      }
    }
    return null;
  }
}
