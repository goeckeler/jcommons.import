package org.jcommons.db.column;

import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * Utility class to retrieve and match meta columns in a list of meta columns
 *
 * @author Thorsten Goeckeler
 */
public final class MetaColumnUtils
{
  /** hide sole constructor */
  private MetaColumnUtils() {
  }

  /**
   * Retrieve a matching meta column from the given list.
   *
   * @param name the case-insensitive name of the column we are interested in
   * @param columns the list of meta columns in which we search
   * @return <code>null</code> if no matching column can be found, otherwise the respective meta column
   */
  public static MetaColumn findByColumnName(final String name, final List<MetaColumn> columns) {
    if (StringUtils.isBlank(name)) return null;
    if (columns == null || columns.isEmpty()) return null;

    String columnName = name.trim();
    for (MetaColumn column : columns) {
      if (columnName.equalsIgnoreCase(column.getName())) {
        return column;
      }
    }

    return null;
  }
}
