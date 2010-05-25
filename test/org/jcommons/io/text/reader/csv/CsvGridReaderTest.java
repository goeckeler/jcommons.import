package org.jcommons.io.text.reader.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.List;

import org.jcommons.io.text.Grid;
import org.jcommons.io.text.reader.GridReader;
import org.jcommons.lang.clazz.ClassUtils;
import org.junit.Test;

/**
 * Check if CSV files can be read
 *
 * @author Thorsten Goeckeler
 */
public class CsvGridReaderTest
{
  private static final String ROOT = ClassUtils.getPackagePath(CsvGridReaderTest.class, "./test");
  private static final String SIMPLE = ROOT + "/simple.csv";
  private static final String SEMICOLON = ROOT + "/semicolon.csv";

  /** check if we can process some simple input files */
  @Test
  public void testRead() {
    GridReader gridReader = new CsvGridReader(SIMPLE);
    Grid table = gridReader.read();
    assertNotNull(table);
    assertEquals(5, table.size());
    assertEquals("b", table.getValue(0, 1));
    assertEquals("b1", table.getValue(1, 1));
    assertEquals("c1, e1", table.getValue(1, 2));
    assertNull(table.getValue(2, 2));
    assertEquals("d2", table.getValue(2, 3));
    assertEquals("b4", table.getValue(4, 1));
  }

  /** check if we can process input files with different parameters */
  @Test
  public void testReadSemicolon() {
    CsvGridReader reader = new CsvGridReader().setDelimiter(";").setEscape("\'");
    reader.setFile(null);
    Grid table = reader.read();
    // no exception thrown!
    assertNull(table);

    // now test real file
    reader.setFile(new File(SEMICOLON));
    assertNotNull(reader.getFile());
    assertEquals(SEMICOLON, reader.getFile().getPath().replace('\\', '/'));

    table = reader.read();
    assertNotNull(table);
    assertEquals(4, table.size());
    assertEquals("b", table.getValue(0, 1));
    assertEquals("\"b1\"", table.getValue(1, 1));
    assertEquals("c1; e1", table.getValue(1, 2));
    assertEquals("b2,,d2", table.getValue(2, 1));
    assertNull(table.getValue(2, 2));
    // test quoting at end of token
    // if string was not quoted this means that nothing is quoted
    assertEquals("b4\'\'", table.getValue(3, 1));
    // if string was quoted then accept double quote
    assertEquals("a3\'", table.getValue(3, 0));
    // test quoting within the token
    assertEquals("c4\'e4", table.getValue(3, 2));
  }

  /** check if that default settings are working */
  @Test
  public void testReadDefault() {
    CsvGridReader reader = new CsvGridReader(new File(SIMPLE)).setDelimiter(null).setEscape(null);
    Grid table = reader.read();
    assertNotNull(table);
    assertEquals(5, table.size());
    assertEquals("b", table.getValue(0, 1));
    assertEquals("b1", table.getValue(1, 1));
    assertEquals("c1, e1", table.getValue(1, 2));
    assertNull(table.getValue(2, 2));
    assertEquals("d2", table.getValue(2, 3));
    assertEquals("b4", table.getValue(4, 1));
  }

  /** check if the row conversions works as expected with the default settings */
  @Test
  public void testToRow() {
    String simple = "a,b,c,d";
    String quoted = "a1, \"b1\" ,\"c1, e1\",d1";
    String empty = "a2, b2,,d2";
    String trailing = "a3, b3,,";

    CsvGridReader reader = new CsvGridReader();

    List<String> columns = reader.toRow(simple);
    assertNotNull(columns);
    assertEquals(4, columns.size());
    assertEquals("a", columns.get(0));
    assertEquals("b", columns.get(1));
    assertEquals("c", columns.get(2));
    assertEquals("d", columns.get(3));

    columns = reader.toRow(quoted);
    assertNotNull(columns);
    assertEquals(4, columns.size());
    assertEquals("a1", columns.get(0));
    assertEquals("b1", columns.get(1));
    assertEquals("c1, e1", columns.get(2));
    assertEquals("d1", columns.get(3));

    columns = reader.toRow(empty);
    assertNotNull(columns);
    assertEquals(4, columns.size());
    assertEquals("a2", columns.get(0));
    assertEquals("b2", columns.get(1));
    assertNull(columns.get(2));
    assertEquals("d2", columns.get(3));

    columns = reader.toRow(trailing);
    assertNotNull(columns);
    assertEquals(4, columns.size());
    assertEquals("a3", columns.get(0));
    assertEquals("b3", columns.get(1));
    assertNull(columns.get(2));
    assertNull(columns.get(3));
  }
}
