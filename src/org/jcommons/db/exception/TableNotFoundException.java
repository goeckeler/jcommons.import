package org.jcommons.db.exception;

import java.sql.SQLException;
import java.text.MessageFormat;

import org.apache.commons.lang.StringUtils;

/**
 * Simply indicates that the given table does not exist.
 *
 * @author Thorsten Goeckeler
 */
public class TableNotFoundException
  extends SQLException
{
  private static final long serialVersionUID = -1764942650385506377L;
  private static final MessageFormat TEXT = new MessageFormat("No such table \"{0}\".");

  /**
   * Create an exception for the given table name
   *
   * @param tableName the table that was not found
   */
  public TableNotFoundException(final String tableName) {
    super(TEXT.format(new Object[] { StringUtils.defaultIfEmpty(tableName, "?") }));
  }

  /**
   * Create an exception for the given table name including the original exception
   *
   * @param tableName the table that was not found
   * @param cause the exception that originally caused the error
   */
  public TableNotFoundException(final String tableName, final Throwable cause) {
    super(TEXT.format(new Object[] { StringUtils.defaultIfEmpty(tableName, "?") }), cause);
  }
}
