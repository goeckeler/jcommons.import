package org.jcommons.io.text;

import java.util.*;

/**
 * A grid is an in-memory representation of an spreadsheet.
 *
 * A grid holds data in columns and rows without putting any semantics in it. You can add, remove and read data as you
 * like. However, the basic data store is based on strings, that is all values are strings, and whoever uses this grid
 * must interpret what kind of value the respective value or column should be. Use a <code>Sheet</code> or
 * <code>Table</code> to interpret columns, rows and data types.
 *
 * @author Thorsten Goeckeler
 */
public class Grid
  implements MutableTabular
{
  /** defines the minimum number if columns and also the minimum number of columns to extend */
  static final int COLUMNS_EXTEND = 10;

  private LinkedList<List<String>> rows = null;

  /** construct an empty table */
  public Grid() {
    this(null);
  }

  /**
   * Constructs a table with a deep copy of the given data
   *
   * @param data the table contents, can be null for no data
   */
  public Grid(final List<List<String>> data) {
    if (data != null) {
      rows = new LinkedList<List<String>>(data);
    } else {
      rows = new LinkedList<List<String>>();
    }
  }

  /**
   * Constructs a table with the given initial capacity for larger tables.
   *
   * @param capacity the number of expected rows for this table
   */
  public Grid(final int capacity) {
    // linked lists extend easily, so we don't need an initial capacity
    rows = new LinkedList<List<String>>();
  }

  /** @return the plain data of this table in read-only mode */
  public List<List<String>> getData() {
    return Collections.unmodifiableList(rows);
  }

  /**
   * Access the given row
   *
   * @param index the row number, the first row is 0
   * @return the data of the row or an empty list if the row is out of bounds, never null
   */
  public List<String> getRow(final int index) {
    if (index < 0 || index >= rows.size()) { return Collections.emptyList(); }
    return rows.get(index);
  }

  /**
   * Add the given row to the end of the table
   *
   * @param row the row data
   * @return the current table to allow chaining
   */
  public Grid add(final List<String> row) {
    rows.add(row);
    return this;
  }

  /**
   * Insert the given row before the indicated row in the table
   *
   * If the row is out of bounds the missing rows will be automatically added.
   *
   * @param index the row number that will be the next row for the inserted row
   * @param row the row data
   * @return the current table to allow chaining
   */
  public Grid insertBefore(final int index, final List<String> row) {
    // interpret negative index as first row
    int position = Math.max(0, index);

    if (position > rows.size()) {
      // add empty rows but the last one as we already have that one
      createEmptyRows(position - 1);
    }

    if (position == 0) {
      rows.addFirst(row);
    } else if (position == rows.size()) {
      rows.addLast(row);
    } else {
      rows.add(position, row);
    }

    return this;
  }

  /**
   * Insert the given row after the indicated row in the table
   *
   * If the row is out of bounds the missing rows will be automatically added.
   *
   * @param index the row number that will be the previous row for the inserted row
   * @param row the row data
   * @return the current table to allow chaining
   */
  public Grid insertAfter(final int index, final List<String> row) {
    return insertBefore(index + 1, row);
  }

  /**
   * Remove the given row from the table
   *
   * If the index is out of bounds nothing will happen.
   *
   * @param index the row number to be removed
   * @return the current table to allow chaining
   */
  public Grid remove(final int index) {
    if (!rows.isEmpty() && index >= 0 && index < rows.size()) {
      rows.remove(index);
    }
    return this;
  }

  /**
   * Retrieve the value of the given row and column
   *
   * If row or column are out of bounds will simply return <code>null</code> to indicate that no value has been set.
   *
   * @param row the row number in the table starting with 0
   * @param column the column number in the table starting with 0
   * @return the associated value of that cell, can be null
   */
  public String getValue(final int row, final int column) {
    if (row < 0 || row >= rows.size()) return null;
    if (column < 0 || rows.get(row).size() <= column) return null;
    return rows.get(row).get(column);
  }

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
  public String setValue(final int row, final int column, final String value) {
    if (row < 0 || column < 0) return null;

    if (row >= rows.size()) {
      if (value != null) {
        createEmptyRows(row);
      } else {
        // out of scope anyway
        return null;
      }
    }

    List<String> data = getRow(row);
    if (column >= data.size()) {
      if (value != null) {
        data = createEmptyColumns(row, column);
      } else {
        // out of scope anyway
        return null;
      }
    }

    return data.set(column, value);
  }

  /** @return the number if current rows of this table */
  public int size() {
    return rows.size();
  }

  /**
   * Resets the table so that it does not contain any data
   *
   * @return the current table
   */
  public Grid clear() {
    rows.clear();
    return this;
  }

  /**
   * Fills in empty rows from the end of the table to the given row
   *
   * @param lastRow the last row that needs to be created
   */
  private void createEmptyRows(final int lastRow) {
    if (lastRow > 0 && lastRow >= rows.size()) {
      for (int count = rows.size(); count <= lastRow; ++count) {
        rows.add(new ArrayList<String>());
      }
    }
  }

  /**
   * Fills in empty columns from the end of the row until the given column
   *
   * @param row the existing row (!) to append to
   * @param lastColumn the last column that must be included, greater 0
   * @return the respective row
   */
  private List<String> createEmptyColumns(final int row, final int lastColumn) {
    // automatically extend the list if needed
    List<String> current = getRow(row);

    if (lastColumn - current.size() > COLUMNS_EXTEND) {
      // makes more sense to re-allocate the whole row now
      List<String> copy = new ArrayList<String>(Math.max(lastColumn + 1, COLUMNS_EXTEND));
      copy.addAll(current);
      rows.set(row, copy);
      current = getRow(row);
    }

    if (lastColumn >= current.size()) {
      // just add a few empty cells to the existing row, hoping that re-allocation is not necessary
      for (int count = current.size(); count <= lastColumn; ++count) {
        current.add(null);
      }
    }

    return current;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (rows == null) return "[]";
    return rows.toString();
  }
}
