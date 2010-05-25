package org.jcommons.db.load.sort;

import java.util.Collections;
import java.util.List;

import org.jcommons.io.sheet.Sheet;

/**
 * A simple sorting strategy that does not sort but returns the same sheets.
 *
 * @author Thorsten Goeckeler
 */
public class SimpleSheetSorter
  implements SheetSortingStrategy
{
  /** {@inheritDoc} */
  @Override
  public List<Sheet> sort(final List<Sheet> sheets) {
    if (sheets == null) return Collections.emptyList();
    return sheets;
  }
}
