package org.jcommons.io.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.*;

import org.junit.*;

/** Check that default spreadsheet implementation for a table holds the contract */
public class SpreadsheetTest
{
  private Grid empty;
  private Grid columnsOnly;
  private Grid oneLine;
  private Grid complex;

  private Spreadsheet sheet;

  private static final String[][] GRID =
      { { "header" }, { "header" }, { "column" }, { "trailer" }, { "data" }, { "moreData" }, { "footer" } };

  /** setup test grids */
  @Before
  public void setup() {
    empty = new Grid();
    columnsOnly = new Grid();
    oneLine = new Grid();
    complex = new Grid();

    columnsOnly.add(Arrays.asList(GRID[2]));
    oneLine.add(Arrays.asList(GRID[2]));
    oneLine.add(Arrays.asList(GRID[4]));

    for (String[] row : GRID) {
      complex.add(Arrays.asList(row));
    }

    sheet = new Spreadsheet();
  }

  /** get rid of test grids */
  @After
  public void tearDown() {
    empty = null;
    columnsOnly = null;
    oneLine = null;
    complex = null;
  }

  /** check that we can populate a spreadsheet right away */
  @Test
  public void testSpreadsheetGrid() {
    sheet = new Spreadsheet(complex).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(1);
    assertNotNull(sheet);
    assertNotNull(sheet.getGrid());
    assertEquals(2, sheet.size());

    sheet.setGrid(null);
    assertEquals(0, sheet.size());
  }

  /** check that we can retrieve column names */
  @Test
  public void testGetColumn() {
    assertEquals(1, sheet.setGrid(oneLine).size());
    assertEquals("column", sheet.getColumn(0));
    assertNull(sheet.getColumn(20));
    assertNull(sheet.getColumn(-20));
  }

  /** check if we can determine all columns */
  @Test
  public void testGetColumns() {
    List<String> columns = sheet.setGrid(columnsOnly).getColumns();
    assertNotNull(columns);
    assertEquals(1, columns.size());
    assertEquals("column", columns.get(0));

    columns = sheet.setGrid(complex).getColumns();
    assertNotNull(columns);
    assertEquals(1, columns.size());
    assertEquals("header", columns.get(0));

    columns = sheet.setSkipHeader(2).getColumns();
    assertNotNull(columns);
    assertEquals(1, columns.size());
    assertEquals("column", columns.get(0));

    columns = sheet.setGrid(empty).getColumns();
    assertNotNull(columns);
    assertEquals(0, columns.size());
  }

  /** check that we can access the data directly */
  @Test
  public void testGetData() {
    // start with indirect test
    assertEquals("[], []", sheet.toString());
    assertEquals("[], []", sheet.setGrid(empty).toString());
    assertEquals("[column], []", sheet.setGrid(columnsOnly).toString());
    assertEquals("[column], [[data]]", sheet.setGrid(oneLine).toString());

    // now access directly with valid data
    sheet.setGrid(complex).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(1);
    List<List<String>> data = sheet.getData();
    assertNotNull(data);
    assertEquals(sheet.size(), data.size());
    assertEquals(2, data.size());
    assertNotNull(data.get(0));
    assertNotNull(data.get(1));
    assertEquals("data", data.get(0).get(0));
    assertEquals("moreData", data.get(1).get(0));

    // and now with invalid data
    sheet.setGrid(oneLine).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(1);
    data = sheet.getData();
    assertNotNull(data);
    assertEquals(sheet.size(), data.size());
    assertEquals(0, data.size());
    sheet.setSkipHeader(0).setSkipFooter(0).setSkipTrailer(0);
    data = sheet.getData();
    assertNotNull(data);
    assertEquals(sheet.size(), data.size());
    assertEquals(1, data.size());
    assertNotNull(data.get(0));
    assertEquals("data", data.get(0).get(0));
  }

  /** check that determination of first and last data row works fail safe */
  @Test
  public void testGetDataRows() {
    assertEquals(-1, sheet.setGrid(null).getDataRow());
    assertEquals(-1, sheet.getLastDataRow());
    assertEquals(1, sheet.setGrid(oneLine).getDataRow());
    assertEquals(1, sheet.getLastDataRow());
    assertEquals(1, sheet.setGrid(complex).getDataRow());
    assertEquals(6, sheet.getLastDataRow());
    assertEquals(-1, sheet.setSkipHeader(4).setSkipTrailer(4).setSkipFooter(4).getDataRow());
    assertEquals(-1, sheet.getLastDataRow());
  }

  /** check that we can access any row in a fail safe manner */
  @Test
  public void testGetRow() {
    // no grid
    List<String> data = sheet.getRow(0);
    assertNotNull(data);
    assertTrue(data.isEmpty());

    // empty grid
    data = sheet.setGrid(empty).getRow(0);
    assertNotNull(data);
    assertTrue(data.isEmpty());

    // out of bounds tests
    data = sheet.setGrid(oneLine).getRow(0);
    assertNotNull(data);
    assertEquals(1, data.size());
    assertEquals("data", data.get(0));

    data = sheet.getRow(-20);
    assertNotNull(data);
    assertTrue(data.isEmpty());

    data = sheet.getRow(20);
    assertNotNull(data);
    assertTrue(data.isEmpty());

    data = sheet.setSkipHeader(2).getRow(0);
    assertNotNull(data);
    assertTrue(data.isEmpty());

    // check that data is still okay
    data = sheet.setSkipHeader(0).getRow(0);
    assertNotNull(data);
    assertEquals(1, data.size());
    assertEquals("data", data.get(0));
  }

  /** check that we can access values individually */
  @Test
  public void testGetValue() {
    // use positional access
    assertNull(sheet.getValue(0, 0));
    assertNull(sheet.setGrid(empty).getValue(0, 0));
    assertEquals("data", sheet.setGrid(complex).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(1).getValue(0, 0));
    assertEquals("moreData", sheet.getValue(1, 0));

    // column out of bounds
    assertNull(sheet.getValue(1, 20));
    assertNull(sheet.getValue(1, -20));

    // row out of bounds
    assertNull(sheet.getValue(20, 0));
    assertNull(sheet.getValue(-15, 0));

    // use named access
    assertEquals("data", sheet.getValue("column", 0));
    assertEquals("moreData", sheet.getValue("column", 1));
    assertNull(sheet.getValue(null, 0));
    assertNull(sheet.getValue("goggles", 0));
    assertNull(sheet.getValue("column", 20));
    assertNull(sheet.getValue("column", -10));
  }

  /** check that we can actually retrieve the column */
  @Test
  public void testIndexOf() {
    sheet = new Spreadsheet(complex).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(1);
    assertNotNull(sheet);
    assertNotNull(sheet.getGrid());
    assertEquals(2, sheet.size());

    assertEquals(0, sheet.indexOf("column"));
    assertEquals(0, sheet.indexOf("CoLuMn"));
    assertEquals(-1, sheet.indexOf("no.such.column"));
  }

  /** check that we can define the column names ourself */
  @Test
  public void testSetColumn() {
    // cannot name columns on empty grids
    assertNull(sheet.setGrid(empty).setColumn(0, "mycolumn").getColumn(0));
    // positive check
    assertEquals("mycolumn", sheet.setGrid(columnsOnly).setColumn(0, "mycolumn").getColumn(0));
    assertEquals(0, sheet.indexOf("mycolumn"));
    assertEquals(-1, sheet.indexOf("column"));
    // out of bounds checks
    assertNull(sheet.setColumn(4, "mycolumn").getColumn(4));
    assertNull(sheet.setColumn(-2, "mycolumn").getColumn(-2));
  }

  /** check that size is computed correctly */
  @Test
  public void testSize() {
    assertEquals(0, sheet.setGrid(empty).size());
    assertEquals(0, sheet.setGrid(columnsOnly).size());
    assertEquals(1, sheet.setGrid(oneLine).size());
    assertEquals(6, sheet.setGrid(complex).size());
    assertEquals(4, sheet.setSkipHeader(2).size());
    assertEquals(3, sheet.setSkipFooter(1).size());
    assertEquals(2, sheet.setSkipTrailer(1).size());
  }

  /** test that arguments are created correctly */
  @Test
  public void testParameters() {
    Map<String, String> parameters = sheet.setGrid(empty).getParameters();
    assertNotNull(parameters);
    assertFalse(parameters.isEmpty());
    assertEquals("0", parameters.get("header"));
    assertEquals("0", parameters.get("trailer"));
    assertEquals("0", parameters.get("footer"));

    parameters = sheet.setGrid(complex).setSkipHeader(2).setSkipFooter(1).setSkipTrailer(3).getParameters();
    assertEquals("2", parameters.get("header"));
    assertEquals("3", parameters.get("trailer"));
    assertEquals("1", parameters.get("footer"));
    assertEquals("Spreadsheet", parameters.get("class"));
  }
}
