package org.jcommons.db.column.converter;

import org.apache.commons.lang.math.NumberUtils;
import org.jcommons.db.column.MetaColumn;
import org.jcommons.message.*;

/** tries to convert the current object into a numeric value */
public class ToNumber
  extends AbstractToValue<Number>
{
  private static final String OVERFLOW = "Value \"${value}\" is too large for ${table}.${column} and will be ignored.";
  private static final String ROUNDED =
      "Mantissa of \"${value}\" is too large for ${table}.${column}. Will round value to ${fraction} digits.";
  private static final String OUTNUMBERED =
      "\"${value}\" is not a valid number for ${table}.${column} and will be ignored.";

  /** {@inheritDoc} */
  @Override
  protected Number objectOf(final MetaColumn meta, final String value, final Message validation) {
    if (meta.isNumeric()) {
      Number number = null;
      try {
        number = NumberUtils.createNumber(value);
        number = checkOverflow(meta, number, validation);
        if (number != null) number = checkMantissa(meta, number, validation);
        return number;
      } catch (NumberFormatException nfe) {
        // will be regarded as null value anyway
        validation.add(new Fault(message(OUTNUMBERED, meta, value).toString()));
      }
    }
    return null;
  }

  private Number checkOverflow(final MetaColumn meta, final Number value, final Message validation) {
    int integral = (Long.valueOf(value.longValue())).toString().length();
    if (integral > meta.getPrecision()) {
      validation.add(new Fault(message(OVERFLOW, meta, value).toString()));
      return null;
    }
    return value;
  }

  private Number checkMantissa(final MetaColumn meta, final Number value, final Message validation) {
    int length = value.toString().length();
    // the integral part was already checked, so we don't need to extract the mantissa first
    if (length > meta.getSize()) {
      validation.add(new Warning(message(ROUNDED, meta, value).toString()));
    }
    return value;
  }
}
