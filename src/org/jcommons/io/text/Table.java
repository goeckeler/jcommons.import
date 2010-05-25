package org.jcommons.io.text;

import java.util.List;
import java.util.Map;

/**
 * A table is an interpreted in-memory representation of an spreadsheet with named columns in read-only mode.
 *
 * A table holds data in columns and rows and interprets and underlying grid in a manner that one particular row names
 * the columns (the column headers) and a particular range constitutes the data. Some rows may be skipped and not
 * interpreted.
 *
 * A table does not interpret the data itself, that is all values are strings, and whoever uses this table must
 * interpret what kind of value the respective value or column should be.
 *
 * A <code>Sheet</code> names a table and can optionally interpret the data.
 *
 * Attention: the first row for a table is the first row of the data section and does not include the column headings.
 * Thus all methods of <code>Tabular</code> return slightly different data here than a <code>Grid</code>.
 *
 * @author Thorsten Goeckeler
 */
public interface Table
  extends Tabular
{
  /** @return the names of the columns in this table or an empty list if no columns have been defined, never null */
  List<String> getColumns();

  /**
   * Define the name of a column
   *
   * @param index the position in the header starting with 0
   * @param name the name to be used for this column
   * @return this to allow chaining
   */
  Table setColumn(int index, String name);

  /**
   * Determine the name of a column if the position is known.
   *
   * @param index the position in the header starting with 0
   * @return the column name at the given position, null if unknown
   */
  String getColumn(int index);

  /**
   * Determine the index of a column if the name is known.
   *
   * @param column the case insensitive name of the respective column
   * @return the index of the column in the headings, -1 if the column is unknown
   */
  int indexOf(String column);

  /**
   * Retrieve the value of the given row and column
   *
   * If the row is out of bounds or the column is unknown will simply return <code>null</code> to indicate that no value
   * has been set.
   *
   * @param column the column name in the table
   * @param row the row number in the table starting with 0
   * @return the associated value of that cell, can be null
   */

  String getValue(String column, int row);

  /**
   * Retrieve the configuration parameters of this table.
   *
   * As there can be many implementations of a table and these implementations are configured in a different manner,
   * this is the place where these parameters can be retrieved in a uniform manner and passed to other layers, e.g. to
   * configure appropriate writers and readers or simply to setup the respective factories.
   *
   * @return the not-null list of (key, value) pairs that configure this particular table
   */
  Map<String, String> getParameters();
}
