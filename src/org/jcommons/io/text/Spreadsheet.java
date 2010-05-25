package org.jcommons.io.text;

import java.util.*;

/**
 * Default implementation of a table.
 *
 * You can provide multiple implementations for the table interface, this one should do for most files. It assumes that
 * nearly all import files and tables fit the following description:
 *
 * <pre>
 *   (lines to skip) / skipHeader
 *   column names
 *   (lines to skip) / skipTrailer
 *   data cells
 *   (lines to skip) / skipFooter
 * </pre>
 *
 * The skipped lines are optional and must not be present. Although they will be read in, you will have no chance to
 * change them. If the need arises to write files as well, the appropriate writer should access the underlying grid.
 *
 * @author Thorsten Goeckeler
 */
public class Spreadsheet
  implements Table
{
  private Grid grid;

  private int skipHeader = 0;
  private int skipTrailer = 0;
  private int skipFooter = 0;

  /** Constructs an empty spreadsheet. */
  public Spreadsheet() {
    this(null);
  }

  /**
   * Constructs a wrapper around the given grid.
   *
   * @param grid the data grid to use
   */
  public Spreadsheet(final Grid grid) {
    this.grid = grid;
  }

  /** {@inheritDoc} */
  @Override
  public String getColumn(final int index) {
    if (index < 0) return null;

    List<String> columns = getColumns();
    if (columns == null || columns.size() <= index) return null;
    return columns.get(index);
  }

  /** @return the row index of the row containing the columns or -1 if this row does not exist */
  protected final int getColumnRow() {
    return (grid == null || grid.size() <= getSkipHeader() ? -1 : getSkipHeader());
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getColumns() {
    int columnRow = getColumnRow();
    if (columnRow == -1) return Collections.emptyList();
    return getGrid().getRow(columnRow);
  }

  /** {@inheritDoc} */
  @Override
  public List<List<String>> getData() {
    if (grid != null && getDataRow() >= 0) {
      return grid.getData().subList(getDataRow(), getLastDataRow() + 1);
    }
    return Collections.emptyList();
  }

  /** @return the row index of the first row containing data or -1 if this row does not exist */
  protected final int getDataRow() {
    int offset = 1 + getSkipHeader() + getSkipTrailer();
    return (grid == null || grid.size() <= offset ? -1 : offset);
  }

  /** @return the row index of the last row containing data or -1 if this row does not exist */
  protected final int getLastDataRow() {
    int firstRow = getDataRow();
    if (firstRow >= 0) {
      // okay, at least we have some data
      if (grid.size() - firstRow - getSkipFooter() > 0) {
        // and there is at least one line that is not skipped, which can be the first line of data
        return grid.size() - getSkipFooter() - 1;
      }
    }
    return -1;
  }

  /** @return the plain data, can be null */
  public Grid getGrid() {
    return grid;
  }

  /** @return sum of all lines hidden from the grid for this table, includes skipped lines and the column row */
  protected final int getHiddenRows() {
    return getSkipHeader() + getSkipTrailer() + getSkipFooter() + 1;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getRow(final int index) {
    if (index < 0 || index >= size() || getDataRow() == -1) return Collections.emptyList();
    return grid.getRow(getDataRow() + index);
  }

  /** @return the number of lines skipped at the end of the grid */
  public int getSkipFooter() {
    return skipFooter;
  }

  /** @return the number of lines skipped at the header of the grid */
  public int getSkipHeader() {
    return skipHeader;
  }

  /** @return the number of lines skipped after the columns row and before the data cells */
  public int getSkipTrailer() {
    return skipTrailer;
  }

  /** {@inheritDoc} */
  @Override
  public String getValue(final int row, final int column) {
    // check if row is out of bounds
    if (row < 0 || row >= size() || column < 0) return null;
    return grid.getValue(getDataRow() + row, column);
  }

  /** {@inheritDoc} */
  @Override
  public String getValue(final String column, final int row) {
    return getValue(row, indexOf(column));
  }

  /** {@inheritDoc} */
  @Override
  public int indexOf(final String column) {
    List<String> columns = getColumns();
    for (int index = 0; index < columns.size(); ++index) {
      if (columns.get(index).equalsIgnoreCase(column)) {
        return index;
      }
    }
    return -1;
  }

  /** {@inheritDoc} */
  @Override
  public Table setColumn(final int index, final String name) {
    int columnRow = getColumnRow();

    if (index >= 0 && columnRow >= 0) {
      if (getGrid().getRow(columnRow).size() > index) {
        getGrid().setValue(columnRow, index, name);
      }
    }

    return this;
  }

  /**
   * Inject the underlying grid that holds the data
   *
   * @param grid the data grid to use
   * @return this to allow chaining
   */
  public Spreadsheet setGrid(final Grid grid) {
    this.grid = grid;
    return this;
  }

  /**
   * Define how many lines are skipped at the end of the grid to ignore footer lines
   *
   * @param skip the number of lines skipped at the end of the grid or 0 to skip none
   * @return this to allow chaining
   */
  public Spreadsheet setSkipFooter(final int skip) {
    this.skipFooter = Math.max(0, skip);
    return this;
  }

  /**
   * Define how many lines are skipped at the start of the grid to determine where the columns are
   *
   * @param skip the number of lines skipped at the header of the grid or 0 to skip none
   * @return this to allow chaining
   */
  public Spreadsheet setSkipHeader(final int skip) {
    this.skipHeader = Math.max(0, skip);
    return this;
  }

  /**
   * Define how many lines are skipped after the columns row and before the data cells
   *
   * @param skip the number of lines to skip or 0 to skip none
   * @return this to allow chaining
   */
  public Spreadsheet setSkipTrailer(final int skip) {
    this.skipTrailer = Math.max(0, skip);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    if (getGrid() == null) return 0;
    // the size are the real data records w/o all hidden rows
    return Math.max(0, grid.size() - getHiddenRows());
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getParameters() {
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put("header", Long.toString(getSkipHeader()));
    parameters.put("trailer", Long.toString(getSkipTrailer()));
    parameters.put("footer", Long.toString(getSkipFooter()));
    parameters.put("class", this.getClass().getSimpleName());
    return parameters;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (grid == null) return "[], []";
    StringBuilder text = new StringBuilder();
    text.append(getColumns().toString()).append(", ");
    text.append(getData().toString());
    return text.toString();
  }
}
