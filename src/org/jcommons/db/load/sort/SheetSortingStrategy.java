package org.jcommons.db.load.sort;

import java.util.List;

import javax.sql.DataSource;

import org.jcommons.io.sheet.Sheet;
import org.jcommons.message.Message;

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

  /** @return the validation messages for this sheet sequence */
  Message validate();

  /** @return the currently used data source */
  DataSource getDataSource();

  /**
   * Inject the data source to be used to access the database.
   *
   * @param dataSource the database connection to use to load the data
   */
  void setDataSource(final DataSource dataSource);
}
