package org.jcommons.io.text.factory;

import java.util.Map;

import org.jcommons.io.text.Grid;
import org.jcommons.io.text.Table;


/**
 * Factory method to easily support multiple implementations for a <code>Table</code>.
 *
 * @author Thorsten Goeckeler
 */
public interface TableFactoryMethod
{
  /**
   * Create a concrete table view on the given grid for the given parameters.
   *
   * @param grid the grid to be contained in the table, can be null but that is senseless
   * @param parameters the parameters that configure the table
   * @return a matching table implementation, never null, per default as <code>Spreadsheet</code>
   */
  Table create(final Grid grid, final Map<String, String> parameters);
}

