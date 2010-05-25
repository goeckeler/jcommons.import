package org.jcommons.db.load;

import static org.jcommons.db.junit.DataSourceFactory.createMemoryDataSource;
import static org.jcommons.lang.clazz.ClassUtils.getPackagePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.dbutils.QueryRunner;
import org.jcommons.db.jdbc.QueryUtils;
import org.jcommons.db.load.meta.MetaTable;
import org.jcommons.io.sheet.Book;
import org.jcommons.io.sheet.Sheet;
import org.jcommons.io.text.reader.csv.CsvBookReader;
import org.jcommons.message.Message;
import org.junit.*;

/**
 * Checks if we can load multiple sheets into the database.
 *
 * @author Thorsten Goeckeler
 */
public class DatabaseLoaderTest
{
  private static final String ROOT = getPackagePath(DatabaseLoaderTest.class, "./test");

  private static final List<String> TABLES = new ArrayList<String>();
  private static final Map<String, String> CREATE = new HashMap<String, String>();

  static {
    TABLES.add("language");
    TABLES.add("roles");
    TABLES.add("role_name");

    CREATE.put("language", "language_id integer not null, name varchar(40) not null");
    CREATE.put("roles", "role_id integer not null, name varchar(40) not null");
    CREATE.put("role_name", "role_id integer not null, language_id integer not null, name varchar(40) not null");
  }

  /**
   * setup database
   *
   * @throws SQLException if table cannot be created
   */
  @BeforeClass
  public static void createTables()
    throws SQLException
  {
    QueryRunner query = new QueryRunner(createMemoryDataSource());
    for (String table : TABLES) {
      StringBuilder sql = new StringBuilder("create table ");
      sql.append(table).append(" ( ").append(CREATE.get(table)).append(" )");

      query.update(sql.toString());
    }
  }

  /**
   * tear down database
   *
   * @throws SQLException if table cannot be dropped
   */
  @AfterClass
  public static void dropTables()
    throws SQLException
  {
    QueryRunner query = new QueryRunner(createMemoryDataSource());

    List<String> drops = new ArrayList<String>(TABLES);
    Collections.reverse(drops);

    for (String table : drops) {
      query.update("drop table " + table);
    }
  }

  /**
   * test the database load
   *
   * @throws SQLException if table cannot be imported
   */
  @Test
  public void testLoad()
    throws SQLException
  {
    try {
      DatabaseLoader load = new DatabaseLoader().setDataSource(createMemoryDataSource());
      CsvBookReader reader = new CsvBookReader();

      for (String table : TABLES) {
        reader.addFile(new File(ROOT, table + ".csv"));
      }

      Book book = reader.read();
      book.setName("roles");
      load.load(book);
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Could not execute database load due to:" + ex.getMessage());
    }

    assertEquals(2, QueryUtils.countRows(createMemoryDataSource(), "language"));
    assertEquals(3, QueryUtils.countRows(createMemoryDataSource(), "roles"));
    assertEquals(6, QueryUtils.countRows(createMemoryDataSource(), "role_names"));
  }

  /**
   * test that invalid files are recognized
   *
   * @throws SQLException if table cannot be imported
   */
  @Test
  public void testColumnValidation()
    throws SQLException
  {
    try {
      CsvBookReader reader = new CsvBookReader().addFile(new File(ROOT, "language_miss.csv"));

      Book book = reader.read();
      book.setName("language");

      assertEquals(1, book.getSheets().size());
      Sheet sheet = book.getSheet("language_miss");
      assertNotNull(sheet);

      sheet.setName("language");
      sheet.setColumns(MetaTable.getMetaData(createMemoryDataSource(), sheet.getName()));

      Message errors = sheet.validateColumns();
      assertFalse(errors.isEmpty());
      assertEquals(2, errors.getTexts().size());

      SheetLoader loader = new SheetLoader().setDataSource(createMemoryDataSource());
      try {
        loader.load(sheet);
      } catch (SQLException ex) {
        ex.printStackTrace();
        fail("Column validation did not work in sheet loader, see: " + ex.getMessage());
      }
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Could not execute database load due to:" + ex.getMessage());
    }
  }
}
