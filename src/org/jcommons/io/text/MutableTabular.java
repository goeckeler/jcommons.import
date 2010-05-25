package org.jcommons.io.text;

import java.util.List;

/**
 * A tabular construct that arranges data in columns and rows and can be modified.
 *
 * This is actually a simplification of a tabular representation, as it avoids putting any semantics in the cells. All
 * cells are string values. If you need a view on the table where the data is interpreted, you need to put a view on the
 * data structure.
 *
 * One standard view on tabular data is a table that stores plain data under column headers. If you need this view,
 * please see <code>Table</code>. A representation with plain data is called a <code>Grid</code>.
 *
 * @author Thorsten Goeckeler
 */
public interface MutableTabular
  extends Tabular
{
  /**
   * Add the given row to the end of the table
   *
   * @param row the row data
   * @return the current table to allow chaining
   */
  MutableTabular add(final List<String> row);

  /**
   * Resets the table so that it does not contain any data
   *
   * @return the current table
   */
  MutableTabular clear();

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

  /**
   * Insert the given row after the indicated row in the table
   *
   * If the row is out of bounds the missing rows will be automatically added.
   *
   * @param index the row number that will be the previous row for the inserted row
   * @param row the row data
   * @return the current table to allow chaining
   */
  MutableTabular insertAfter(final int index, final List<String> row);

  /**
   * Insert the given row before the indicated row in the table
   *
   * If the row is out of bounds the missing rows will be automatically added.
   *
   * @param index the row number that will be the next row for the inserted row
   * @param row the row data
   * @return the current table to allow chaining
   */
  MutableTabular insertBefore(final int index, final List<String> row);

  /**
   * Remove the given row from the table
   *
   * If the index is out of bounds nothing will happen.
   *
   * @param index the row number to be removed
   * @return the current table to allow chaining
   */
  MutableTabular remove(final int index);

  /**
   * Set the value of the given row and column
   *
   * If row or column are out of bounds the missing rows and columns will be automatically added if the value is not
   * null.
   *
   * @param row the row number in the table starting with 0
   * @param column the column number in the table starting with 0
   * @param value the value to place into that cell
   * @return the previously associated value of that cell, can be null
   */
  String setValue(final int row, final int column, final String value);

  /** @return the number if current rows of this table */
  int size();
}
