package org.jcommons.io.text;

import java.util.List;

/**
 * A tabular construct that arranges data in columns and rows in read-only fashion.
 *
 * This is actually a simplification of a tabular representation, as it avoids putting any semantics in the cells. All
 * cells are string values. If you need a view on the table where the data is interpreted, you need to put a view on the
 * data structure.
 *
 * One standard view on tabular data is a table that stores plain data under column headers. If you need this view,
 * please see <code>Table</code>. A modifiable representation with plain data is called a <code>Grid</code> which is an
 * implementation of <code>MutableTabular</code>.
 *
 * @author Thorsten Goeckeler
 */
public interface Tabular
{
  /** @return the plain data of this table in read-only mode */
  List<List<String>> getData();

  /**
   * Access the given row
   *
   * @param index the row number, the first row is 0
   * @return the data of the row or an empty list if the row is out of bounds, never null
   */
  List<String> getRow(final int index);

  /**
   * Retrieve the value of the given row and column
   *
   * If row or column are out of bounds will simply return <code>null</code> to indicate that no value has been set.
   *
   * @param row the row number in the table starting with 0
   * @param column the column number in the table starting with 0
   * @return the associated value of that cell, can be null
   */
  String getValue(final int row, final int column);

  /** @return the number if current rows of this table */
  int size();
}
