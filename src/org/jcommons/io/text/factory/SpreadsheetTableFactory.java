package org.jcommons.io.text.factory;

import java.util.Map;

import org.jcommons.io.text.*;


/**
 * Factory method to create a <code>Spreadsheet</code>.
 *
 * @author Thorsten Goeckeler
 */
class SpreadsheetTableFactory
  implements TableFactoryMethod
{
  /**
   * Create a spreadsheet view on the given grid with the given parameters.
   *
   * @param grid the grid to be contained in the table, can be null but that is senseless
   * @param parameters the parameters to configure the spreadsheet
   * @return a matching <code>Spreadsheet</code>, never null
   */
  @Override
  public Table create(final Grid grid, final Map<String, String> parameters) {
    Spreadsheet table = new Spreadsheet(grid);

    if (parameters != null) {
      if (parameters.containsKey("header")) table.setSkipHeader(Integer.valueOf(parameters.get("header")));
      if (parameters.containsKey("footer")) table.setSkipHeader(Integer.valueOf(parameters.get("footer")));
      if (parameters.containsKey("trailer")) table.setSkipHeader(Integer.valueOf(parameters.get("trailer")));
    }

    return table;
  }
}
