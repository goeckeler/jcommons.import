package org.jcommons.db.load.sort;

import java.util.List;

import org.jcommons.io.sheet.Sheet;

/**
 * Sorting strategy so that the sheets are in an optimal order to be loaded.
 *
 * @author Thorsten Goeckeler
 */
public interface SheetSortingStrategy
{
  /**
   * Sort the sheets to that they can be loaded w/o foreign key constraints.
   *
   * @param sheets the sheets to be loaded
   * @return the sorted list of sheets
   */
  List<Sheet> sort(final List<Sheet> sheets);
}
