package org.jcommons.io.text.reader;

import org.jcommons.io.text.Grid;

/**
 * A text reader reads text from some input and presents it in an in-memory format
 */
public interface GridReader
{
  /**
   * Reads table data from some source.
   *
   * @return null if the data cannot be read, otherwise a possible empty table
   */
  Grid read();
}
