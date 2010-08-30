package org.jcommons.db.load.sheet;

import org.jcommons.io.sheet.Sheet;

/**
 * SQL Factory to create the SQL statements to update or insert from a given sheet.
 *
 * @author Thorsten Goeckeler
 */
public class SheetSqlFactory
{
  /** hide sole constructor */
  protected SheetSqlFactory() {
  }

  /**
   * Create SQL command to insert not-null fields into the corresponding database table.
   *
   * @param sheet the sheet to be imported
   * @return the respective SQL command as a prepared statement
   */
  public static String insert(final Sheet sheet) {
    StringBuffer sql = new StringBuffer();
    sql.append("insert into ").append(sheet.getName()).append(" (");

    return sql.toString();
  }

  /**
   * Create SQL command to update only not-null fields into the corresponding database table.
   *
   * @param sheet the sheet to be imported
   * @return the respective SQL command as a prepared statement
   */
  public static String notNullUpdate(final Sheet sheet) {
    // TODO : create update script
    return null;
  }

  /**
   * Create SQL command to update all optional fields into the corresponding database table.
   *
   * @param sheet the sheet to be imported
   * @return the respective SQL command as a prepared statement
   */
  public static String update(final Sheet sheet) {
    // TODO : create update script
    return null;
  }
}
