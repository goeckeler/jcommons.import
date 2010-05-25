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

    for (Sheet sheet : sheets) {
      try {
        dependends.put(sheet.getName().toUpperCase(), MetaTable.dependsOn(getDataSource(), sheet.getName()));
        required.put(sheet.getName().toUpperCase(), MetaTable.dependsMandatoryOn(getDataSource(), sheet.getName()));
      } catch (SQLException ex) {
        NamedString text = message(CANNOT_ACCESS_TABLE);
        text.with("table", sheet.getName()).with("exception", ex.getMessage());
        errors.add(new Fault(text.toString()));
        dependends.remove(sheet.getName().toUpperCase());
        required.remove(sheet.getName().toUpperCase());
      }
    }

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

    // now we have only those tables left for which all dependencies can be resolved
    // TODO : get them in the right sort order, return the correct set of sheets
    return sheets;
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
