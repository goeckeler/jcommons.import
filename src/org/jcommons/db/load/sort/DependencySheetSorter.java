package org.jcommons.db.load.sort;

import java.util.Collections;
import java.util.List;

import org.jcommons.io.sheet.Sheet;

/**
 * A sorting strategy that lists the sheets in the order they depend upon.
 *
 * Can cope with cycle references but you need to lock at the error messages to know whether you can really load the
 * sheets.
 *
 * @author Thorsten Goeckeler
 */
public class DependencySheetSorter
  implements SheetSortingStrategy
{
  /** {@inheritDoc} */
  @Override
  public List<Sheet> sort(final List<Sheet> sheets) {
    // TODO : implement this
    if (sheets == null) return Collections.emptyList();
    return sheets;
  }
}
