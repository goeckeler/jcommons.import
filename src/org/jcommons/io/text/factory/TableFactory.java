package org.jcommons.io.text.factory;

import java.util.HashMap;
import java.util.Map;

import org.jcommons.io.text.*;


/**
 * Factory to create the appropriate table implementation for a given set of parameters
 *
 * @author Thorsten Goeckeler
 */
public final class TableFactory
{
  private static final Map<String, TableFactoryMethod> METHODS = new HashMap<String, TableFactoryMethod>();

  static {
    METHODS.put(Spreadsheet.class.getSimpleName(), new SpreadsheetTableFactory());
  }

  /** hide sole constructor */
  private TableFactory() {
  }

  /**
   * Create a table view on the given grid for the given parameters.
   *
   * @param grid the grid to be contained in the table, can be null but that is senseless
   * @param parameters the parameters that configure the table
   * @return a matching table implementation, never null, per default as <code>Spreadsheet</code>
   */
  public static Table create(final Grid grid, final Map<String, String> parameters) {
    if (parameters == null || !parameters.containsKey("class") || !METHODS.containsKey(parameters.get("class"))) {
      // use default implementation
      return METHODS.get(Spreadsheet.class.getSimpleName()).create(grid, parameters);
    }

    return METHODS.get(parameters.get("class")).create(grid, parameters);
  }
}
