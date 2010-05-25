package org.jcommons.db.load.sort;

import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.jcommons.io.sheet.Sheet;
import org.jcommons.message.Message;
import org.jcommons.message.Messages;

/**
 * A simple sorting strategy that does not sort but returns the same sheets.
 *
 * @author Thorsten Goeckeler
 */
public class SimpleSheetSorter
  implements SheetSortingStrategy
{
  private static Message errors = new Messages();

  /** {@inheritDoc} */
  @Override
  public List<Sheet> sort(final List<Sheet> sheets) {
    if (sheets == null) return Collections.emptyList();
    return sheets;
  }

  /** {@inheritDoc} */
  @Override
  public Message validate() {
    return errors;
  }

  /** {@inheritDoc} */
  @Override
  public DataSource getDataSource() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void setDataSource(final DataSource dataSource) {
  }
}
