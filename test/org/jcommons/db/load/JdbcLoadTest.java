package org.jcommons.db.load;

import static org.jcommons.db.junit.DataSourceFactory.createMemoryDataSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.math.NumberUtils;
import org.jcommons.db.jdbc.QueryUtils;
import org.joda.time.DateTime;
import org.junit.*;


/**
 * Check if table meta data can be queried
 *
 * @author Thorsten Goeckeler
 */
public class JdbcLoadTest
{
  private static final String CREATE_SQL =
      "create table tag ( name varchar(10) not null, married char(1),"
          + " age integer, born date, altered timestamp not null, salary decimal(6,2) )";
  private static final String DROP_SQL = "drop table tag";
  private static final String INSERT_SQL =
      "insert into tag (name, married, age, born, altered, salary) values ( ?, ?, ?, ?, ?, ?)";
  private static final String DELETE_SQL = "delete from tag";

  /**
   * setup database
   *
   * @throws SQLException if table cannot be created
   */
  @BeforeClass
  public static void createTable()
    throws SQLException
  {
    new QueryRunner(createMemoryDataSource()).update(CREATE_SQL);
  }

  /**
   * tear down database
   *
   * @throws SQLException if table cannot be dropped
   */
  @AfterClass
  public static void dropTable()
    throws SQLException
  {
    new QueryRunner(createMemoryDataSource()).update(DROP_SQL);
  }

  /**
   * clear all tables so all tests start with nothing
   *
   * @throws SQLException if table cannot be cleared
   */
  @Before
  public void clearTable()
    throws SQLException
  {
    new QueryRunner(createMemoryDataSource()).update(DELETE_SQL);
  }

  /**
   * test how we can insert records
   *
   * @throws SQLException if table cannot be accessed
   */
  @Test
  public void testInsert()
    throws SQLException
  {
    DataSource dbms = createMemoryDataSource();
    assertEquals(0, QueryUtils.countRows(dbms, "tag"));

    // insert plain
    QueryRunner query = new QueryRunner(dbms);
    query.update("insert into tag (name, altered) values ('alice', sysdate)");
    query.update("insert into tag (name, altered) values ('bob', sysdate)");
    assertEquals(2, QueryUtils.countRows(dbms, "tag"));

    // insert using objects and prepared statement
    // name, married, age, born, altered, salary
    try {
      DateTime born = new DateTime(1996, 7, 2, 10, 35, 0, 0);
      DateTime altered = new DateTime(2010, 5, 19, 9, 30, 52, 0);
      Timestamp stamp = new Timestamp(altered.getMillis());

      query.update(INSERT_SQL, "charly", "0", 11, born.toDate(), stamp, 2345.20);
      assertEquals(3, QueryUtils.countRows(dbms, "tag"));
      query.update(INSERT_SQL, "dick", null, null, null, stamp, BigDecimal.valueOf(1000.00));
      assertEquals(4, QueryUtils.countRows(dbms, "tag"));
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Object insert did not succeed.");
    }

    // insert using generic objects
    // name, married, age, born, altered, salary
    try {
      Object born = new DateTime(1996, 7, 2, 10, 35, 0, 0).toDate();
      Object stamp = new Timestamp(new DateTime(2010, 5, 19, 9, 30, 52, 0).getMillis());
      Object name = "ella";
      Object salary = 25.25;
      Object age = 24;

      query.update(INSERT_SQL, name, null, age, born, stamp, salary);
      assertEquals(5, QueryUtils.countRows(dbms, "tag"));
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Generic object insert did not succeed.");
    }

    // insert unescaped values
    // name, married, age, born, altered, salary
    try {
      DateTime born = new DateTime(1996, 7, 2, 10, 35, 0, 0);
      Timestamp stamp = new Timestamp(new DateTime(2010, 5, 19, 9, 30, 52, 0).getMillis());

      query.update(INSERT_SQL, "da'vid", null, null, born.toDate(), stamp, 1.00);
      query.update(INSERT_SQL, "da\"vid", null, null, born.toDate(), stamp, 1.00);
      assertEquals(7, QueryUtils.countRows(dbms, "tag"));
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Unescaped insert did not succeed.");
    }
  }

  /**
   * test how we can insert records
   *
   * @throws SQLException if table cannot be accessed
   */
  @Test
  public void testBatchInsert()
    throws SQLException
  {
    DataSource dbms = createMemoryDataSource();
    assertEquals(0, QueryUtils.countRows(dbms, "tag"));

    Object stamp = new Timestamp(new DateTime(2000, 5, 19, 9, 30, 52, 0).getMillis());
    // name, married, age, born, altered, salary
    Object[][] data =
        {
          { "alpha", "0", 14, null, stamp, null }, { "bravo", "1", 34, null, stamp, 400.0 },
          { "gamma", "1", 36, null, stamp, NumberUtils.createNumber("800.0") } };

    // insert using batch
    QueryRunner query = new QueryRunner(dbms);
    try {
      query.batch(INSERT_SQL, data);
      assertEquals(3, QueryUtils.countRows(dbms, "tag"));
    } catch (SQLException ex) {
      ex.printStackTrace();
      fail("Batch insert did not succeed.");
    }
  }
}
