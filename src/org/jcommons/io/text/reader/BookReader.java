package org.jcommons.io.text.reader;

import java.util.Map;

import org.jcommons.io.sheet.Book;


/**
 * A data reader that reads text from source presents it in an in-memory format like a book
 */
public interface BookReader
{
  /**
   * Reads multiple table data from some source using a default configuration.
   *
   * @return null if the data cannot be read, otherwise a possible empty book
   */
  Book read();

  /**
   * Reads multiple table data from some source.
   *
   * @param arguments several parameters to configure the reader, can be null
   * @return null if the data cannot be read, otherwise a possible empty book
   */
  Book read(Map<String, String> arguments);
}
