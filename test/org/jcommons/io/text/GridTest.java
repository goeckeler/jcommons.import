package org.jcommons.io.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;

import org.junit.Test;

/** Test grid implementation */
public class GridTest
{
  private final String[] columns = { "A", "B", "C" };
  private final String[][] rows = { { "a1", "b1", "c1" }, { "a2", "b2", "c2" }, { "a3", null, "c3" } };

  /** test default constructor */
  @Test
  public void testGrid() {
    Grid grid = new Grid();
    assertNotNull(grid);
    assertEquals(0, grid.size());
  }

  /** test constructor with data and if data can be retrieved thereafter */
  @Test
  public void testGridWithData() {
    List<List<String>> data = new ArrayList<List<String>>();
    data.add(Arrays.asList(columns));
    for (String[] row : rows) {
      data.add(Arrays.asList(row));
    }

    Grid grid = new Grid(data);

    assertNotNull(grid);
    assertEquals(1 + rows.length, grid.size());

    assertNotNull(grid.getData());
    assertArrayEquals(data.toArray(), grid.getData().toArray());
  }

  /** test default capacity */
  @Test
  public void testGridWithCapacity() {
    Grid grid = new Grid(4);
    assertNotNull(grid);
    assertEquals(0, grid.size());
  }

  /** test single row */
  @Test
  public void testGetRow() {
    Grid grid = new Grid();

    List<String> row = grid.getRow(0);
    assertNotNull(row);
    assertTrue(row.isEmpty());

    try {
      row.add("test");
      fail("Should not be able to add items to an empty row.");
    } catch (UnsupportedOperationException ex) {
      // well, this is okay
    }

    row = grid.getRow(4);
    assertNotNull(row);
    assertTrue(row.isEmpty());

    List<List<String>> data = new ArrayList<List<String>>();
    data.add(Arrays.asList(columns));
    for (String[] dataRow : rows) {
      data.add(Arrays.asList(dataRow));
    }

    grid = new Grid(data);
    row = grid.getRow(0);
    assertNotNull(row);
    assertFalse(row.isEmpty());
    assertEquals(columns.length, row.size());
    assertEquals(columns[0], row.get(0));
    assertEquals(columns[2], row.get(2));

    row = grid.getRow(rows.length);
    assertNotNull(row);
    assertFalse(row.isEmpty());
    assertEquals(rows[rows.length - 1].length, row.size());
    assertEquals(rows[rows.length - 1][0], row.get(0));
    assertEquals(rows[rows.length - 1][2], row.get(2));
  }

  /** test add to end of grid */
  @Test
  public void testAdd() {
    Grid grid = new Grid();
    grid.add(Arrays.asList(columns));
    assertEquals(1, grid.size());
    grid.add(Arrays.asList(rows[0]));
    assertEquals(2, grid.size());

    // check to add an empty row
    grid.add(null);
    assertEquals(3, grid.size());

    // also in right sequence?
    assertEquals(columns[0], grid.getValue(0, 0));
    assertEquals(rows[0][0], grid.getValue(1, 0));
  }

  /** test insert into grid before a row */
  @Test
  public void testInsertBefore() {
    Grid grid = new Grid();
    grid.add(Arrays.asList(rows[0]));

    // check to insert before existing row
    grid.insertBefore(0, Arrays.asList(columns));
    assertEquals(2, grid.size());
    assertEquals(columns[0], grid.getValue(0, 0));
    assertEquals(rows[0][0], grid.getValue(1, 0));

    // check with invalid index
    grid.insertBefore(-1, Arrays.asList(rows[1]));
    assertEquals(3, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(rows[0][0], grid.getValue(2, 0));
    assertEquals(rows[1][0], grid.getValue(0, 0));

    // check if added at end of list
    grid.insertBefore(grid.size(), Arrays.asList(rows[2]));
    assertEquals(4, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(rows[0][0], grid.getValue(2, 0));
    assertEquals(rows[1][0], grid.getValue(0, 0));
    assertEquals(rows[2][0], grid.getValue(3, 0));

    // check if empty rows are added automatically
    grid.insertBefore(7, Arrays.asList(rows[2]));
    assertEquals(8, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(rows[0][0], grid.getValue(2, 0));
    assertEquals(rows[1][0], grid.getValue(0, 0));
    assertEquals(rows[2][0], grid.getValue(3, 0));
    assertEquals(rows[2][0], grid.getValue(7, 0));

    // check to insert in between
    grid.insertBefore(1, Arrays.asList(columns));
    assertEquals(9, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(columns[0], grid.getValue(2, 0));
  }

  /** test insert into grid after a row */
  @Test
  public void testInsertAfter() {
    Grid grid = new Grid();
    grid.add(Arrays.asList(columns));

    // check to insert after existing row
    grid.insertAfter(0, Arrays.asList(rows[0]));
    assertEquals(2, grid.size());
    assertEquals(columns[0], grid.getValue(0, 0));
    assertEquals(rows[0][0], grid.getValue(1, 0));

    // check with invalid index to be added at start of list
    grid.insertAfter(-1, Arrays.asList(rows[1]));
    assertEquals(3, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(rows[0][0], grid.getValue(2, 0));
    assertEquals(rows[1][0], grid.getValue(0, 0));

    // check if added at end of list adding an empty row
    grid.insertAfter(grid.size(), Arrays.asList(rows[2]));
    assertEquals(5, grid.size());
    assertEquals(columns[0], grid.getValue(1, 0));
    assertEquals(rows[0][0], grid.getValue(2, 0));
    assertEquals(rows[1][0], grid.getValue(0, 0));
    assertEquals(rows[2][0], grid.getValue(4, 0));
    assertNull(grid.getValue(3, 0));
  }

  /** test if we can remove a row */
  @Test
  public void testRemove() {
    Grid grid = new Grid();
    grid.add(Arrays.asList(columns));
    for (String[] dataRow : rows) {
      grid.add(Arrays.asList(dataRow));
    }
    assertEquals(1 + rows.length, grid.size());

    // check remove first row
    grid.remove(0);
    assertEquals(rows.length, grid.size());
    assertEquals(rows[0][0], grid.getValue(0, 0));

    // check remove in between
    grid.remove(1);
    assertEquals(rows.length - 1, grid.size());
    assertEquals(rows[0][0], grid.getValue(0, 0));
    assertEquals(rows[2][0], grid.getValue(1, 0));

    // check remove non-existing row
    grid.remove(5);
    assertEquals(rows.length - 1, grid.size());
    assertEquals(rows[0][0], grid.getValue(0, 0));
    assertEquals(rows[2][0], grid.getValue(1, 0));

    // check remove last row
    grid.remove(grid.size() - 1);
    assertEquals(rows.length - 2, grid.size());
    assertEquals(rows[0][0], grid.getValue(0, 0));

    grid.clear();
    try {
      grid.remove(0);
    } catch (Exception ex) {
      fail("Remove on empty grid caused exception: " + ex.getMessage());
    }
  }

  /** test if getValue() is fail safe */
  @Test
  public void testGetValue() {
    Grid grid = new Grid();

    try {
      assertNull(grid.getValue(18, 18));
    } catch (Exception ex) {
      fail("Grid.getValue() on empty grid caused exception: " + ex.getMessage());
    }

    // check that no extra row has been added
    assertEquals(0, grid.size());

    grid.add(Arrays.asList(columns));
    for (String[] dataRow : rows) {
      grid.add(Arrays.asList(dataRow));
    }

    assertEquals(columns[0], grid.getValue(0, 0));
    assertEquals(rows[0][2], grid.getValue(1, 2));
    assertEquals(rows[1][1], grid.getValue(2, 1));
    assertEquals(rows[2][2], grid.getValue(3, 2));
    assertNull(grid.getValue(3, 1));

    // check that out of bound on column cannot occur
    assertNull(grid.getValue(0, columns.length + 1));
  }

  /** test if setValue() is fail safe */
  @Test
  public void testSetValue() {
    Grid grid = new Grid();

    try {
      assertNull(grid.setValue(1, 1, "test"));
    } catch (Exception ex) {
      fail("Grid.setValue() on empty grid caused exception: " + ex.getMessage());
    }

    // check that extra rows have been added
    assertEquals(2, grid.size());

    assertEquals("test", grid.setValue(1, 1, "test2"));
    assertEquals("test2", grid.getValue(1, 1));

    // check that extra rows are not added if not necessary
    grid.setValue(4, 0, null);
    assertEquals(2, grid.size());

    // check that row is extended automatically
    grid.setValue(1, 3, "test");
    assertEquals("test", grid.getValue(1, 3));
    assertEquals(4, grid.getRow(1).size());

    // check that row is extended automatically even for large columns
    int column = grid.getRow(1).size() + Grid.COLUMNS_EXTEND + 1;
    assertNull(grid.setValue(1, column, "test"));
    assertEquals("test", grid.getValue(1, column));

    // check that row is not extended if row exists but not the column
    assertNull(grid.setValue(0, 4, null));
    assertEquals(0, grid.getRow(0).size());

    // check that invalid row/column indices are not causing an error
    try {
      assertNull(grid.setValue(-1, 0, null));
      assertNull(grid.setValue(-1, 0, "test"));
      assertNull(grid.setValue(0, -10, null));
      assertNull(grid.setValue(0, -10, "test"));
      assertNull(grid.setValue(-1, -1, null));
      assertNull(grid.setValue(-1, -1, "test"));
    } catch (Exception ex) {
      fail("Out of bounds shall not occur during setValue(): " + ex.getMessage());
    }
  }

  /** check if we get rid of data */
  @Test
  public void testClear() {
    Grid grid = new Grid();
    grid.add(Arrays.asList(columns));
    assertEquals(1, grid.size());
    assertEquals(0, grid.clear().size());
  }
}
