package org.jcommons.io.text.reader.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.jcommons.io.sheet.Book;
import org.jcommons.lang.clazz.ClassUtils;
import org.junit.Test;

/**
 * Check if multiple CSV files can be read
 *
 * @author Thorsten Goeckeler
 */
public class CsvBookReaderTest
{
  private static final String ROOT = ClassUtils.getPackagePath(CsvBookReaderTest.class, "./test");
  private static final String SIMPLE = ROOT + "/simple.csv";

  /** check that the book can be read completely */
  @Test
  public void testRead() {
    CsvBookReader reader = new CsvBookReader().addFile(new File(SIMPLE));
    Book book = reader.read();
    assertNotNull(book);
    assertNotNull(book.getSheets());
    assertEquals(1, book.getSheets().size());
    assertNotNull(book.getSheets().get(0).getName());
    assertEquals("simple", book.getSheets().get(0).getName());
    assertEquals("a", book.getSheets().get(0).getTable().getColumn(0));
    assertEquals("a1", book.getSheets().get(0).getTable().getValue(0, 0));
  }
}
