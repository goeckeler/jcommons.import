package org.jcommons.db.load.sort;

import static org.jcommons.lang.string.NamedString.message;

import java.sql.SQLException;
import java.util.*;

import javax.sql.DataSource;

import org.jcommons.db.load.meta.MetaTable;
import org.jcommons.io.sheet.Sheet;
import org.jcommons.lang.string.NamedString;
import org.jcommons.message.*;

/**
 * A sorting strategy that lists the sheets in the order they depend upon.
 *
 * Can cope with cycle references but you need to lock at the error messages to know whether you can really load the
 * sheets.
 *
 * @author Thorsten Goeckeler
 */
public class DependencySheetSorter
  implements SheetSortingStrategy
{
  private final Message errors = new Messages();
  private DataSource dataSource;

  private static final String CANNOT_ACCESS_TABLE =
      "Cannot access table \"${table}\" to load the respective sheet due to: ${exception}.";
  private static final String DEPENDS_ON = "Table \"${table}\" depends on table \"${master}\" which is not provided.";
  private static final String DEPENDED_ON = "Table \"${table}\" depends on removed table \"${master}\".";

  /** {@inheritDoc} */
  @Override
  public List<Sheet> sort(final List<Sheet> sheets) {
    errors.clear();
    if (sheets == null) return Collections.emptyList();

    Map<String, Set<String>> dependends = new HashMap<String, Set<String>>();
    Map<String, Set<String>> required = new HashMap<String, Set<String>>();

    getDependencies(sheets, dependends, required);
    Set<String> removed = getTablesWithoutMaster(sheets, required);
    removeTablesWithoutMaster(dependends, required, removed);

    // now we have only those tables left for which all dependencies can be resolved
    return sort(sheets, required, dependends);
  }

  /**
   * Sort the list of sheets in a sequence that allow them to be loaded.
   *
   * May remove sheets that cannot be loaded because their dependencies are missing.
   *
   * @param sheets the original list of sheets, never <code>null</code>
   * @param required the map of tables and their required master tables
   * @param dependends the map of tables and the tables they depend on
   * @return the sorted list of sheets that can be loaded in that sequence
   */
  private List<Sheet> sort(final List<Sheet> sheets, final Map<String, Set<String>> required,
                           final Map<String, Set<String>> dependends)
  {
    List<String> sequence = new LinkedList<String>();

    // the algorithm is simple, as we assume that the sheets are loaded first with all non-nullable fields (which
    // include primary keys and mandatory foreign keys) and then again with the remaining data including optional
    // foreign keys. If those keys are missing, there was no corresponding entry in the respective sheet.

    // while (there is a table entry in the required map)
    //   add all table entries to the list which require no other table
    //   remove these tables from the table map
    //   remove these tables in all requirement lists

    List<String> removed = new LinkedList<String>();
    while (!required.keySet().isEmpty()) {
      removed.clear();
      // add all tables that require no other table
      for (String table : required.keySet()) {
        if (required.get(table).isEmpty()) {
          // no remaining master tables, it is safe to load this table
          sequence.add(table);
          removed.add(table);
        }
      }

      // remove these tables from the table mappings
      for (String table : removed) {
        required.remove(table);
      }

      // now remove them from the remaining requirements lists
      for (String table : required.keySet()) {
        // works only if all tables are written in upper case
        required.get(table).removeAll(removed);
      }
    }
    removed = null;

    // finally match the table list with the sheets
    List<Sheet> sortedSheets = new LinkedList<Sheet>();
    Map<String, Sheet> map = new HashMap<String, Sheet>();
    for (Sheet sheet : sheets) {
      map.put(sheet.getName().toUpperCase(), sheet);
    }

    for (String table : sequence) {
      Sheet sheet = map.get(table);
      // actually all sheets should be mapped, just a bit of paranoia at work
      if (sheet != null) sortedSheets.add(sheet);
    }

    return sortedSheets;
  }

  private void removeTablesWithoutMaster(final Map<String, Set<String>> dependends,
                                         final Map<String, Set<String>> required, final Set<String> removed)
  {
    // now remove those tables and check if we must remove more dependent ones until the list is empty
    while (!removed.isEmpty()) {
      for (String table : removed) {
        dependends.remove(table.toUpperCase());
        required.remove(table.toUpperCase());
      }

      removed.clear();
      for (String table : required.keySet()) {
        for (String requires : required.get(table)) {
          if (!required.containsKey(requires)) {
            NamedString text = message(DEPENDED_ON).with("table", table);
            text.with("master", requires);
            errors.add(new Fault(text.toString()));
            removed.add(requires);
          }
        }
      }
    }
  }

  private Set<String> getTablesWithoutMaster(final List<Sheet> sheets, final Map<String, Set<String>> required) {
    // check for tables that must be present
    Set<String> removed = new HashSet<String>();
    for (Sheet sheet : sheets) {
      for (String requires : required.get(sheet.getName().toUpperCase())) {
        if (!required.containsKey(requires)) {
          NamedString text = message(DEPENDS_ON).with("table", sheet.getName().toUpperCase());
          text.with("master", requires);
          errors.add(new Fault(text.toString()));
          removed.add(requires);
        }
      }
    }
    return removed;
  }

  private void getDependencies(final List<Sheet> sheets, final Map<String, Set<String>> dependends,
                               final Map<String, Set<String>> mandatory)
  {
    for (Sheet sheet : sheets) {
      try {
        dependends.put(sheet.getName().toUpperCase(), MetaTable.dependsOn(getDataSource(), sheet.getName()));
        mandatory.put(sheet.getName().toUpperCase(), MetaTable.dependsMandatoryOn(getDataSource(), sheet.getName()));
      } catch (SQLException ex) {
        NamedString text = message(CANNOT_ACCESS_TABLE);
        text.with("table", sheet.getName()).with("exception", ex.getMessage());
        errors.add(new Fault(text.toString()));
        dependends.remove(sheet.getName().toUpperCase());
        mandatory.remove(sheet.getName().toUpperCase());
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Message validate() {
    return errors;
  }

  /** @return the currently used data source */
  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * Inject the data source to be used to access the database.
   *
   * @param dataSource the database connection to use to load the data
   */
  public void setDataSource(final DataSource dataSource) {
    this.dataSource = dataSource;
  }
}
