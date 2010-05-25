package org.jcommons.db.load.meta;

import static org.jcommons.db.junit.DataSourceFactory.createMemoryDataSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbutils.QueryRunner;
import org.jcommons.db.column.MetaColumn;
import org.jcommons.db.exception.TableNotFoundException;
import org.jcommons.db.jdbc.QueryUtils;
import org.junit.*;

/**
 * Check if table meta data can be queried
 *
 * @author Thorsten Goeckeler
 */
public class MetaTableTest
{
  private static final String CREATE_SQL =
      "create table tag ( name varchar(10) not null, married char(1),"
          + " age integer, born date, altered timestamp not null, salary decimal(6,2), PRIMARY KEY (name) )";
  private static final String DROP_SQL = "drop table tag";

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
   * test meta data retrieval
   *
   * @throws SQLException if table cannot be accessed
   */
  @Test
  public void testGetMetaData()
    throws SQLException
  {
    List<MetaColumn> columns = MetaTable.getMetaData(createMemoryDataSource(), "tag");
    assertNotNull(columns);
    assertEquals(6, columns.size());

    assertEquals("AGE", columns.get(2).getName());
    assertTrue(columns.get(2).isNumeric());

    assertEquals("SALARY", columns.get(5).getName());
    assertTrue(columns.get(5).isNumeric());
    assertEquals(6, columns.get(5).getPrecision().intValue());
    assertEquals(2, columns.get(5).getFraction().intValue());
    assertEquals("PUBLIC.TAG.SALARY[BigDecimal(6,2)]", columns.get(5).toString());

    assertEquals("NAME", columns.get(0).getName());
    assertEquals(10, columns.get(0).getSize().intValue());
    assertFalse(columns.get(0).isNullable());
    assertTrue(columns.get(0).isNotNullable());
    assertTrue(columns.get(0).isPrimary());
    assertEquals("PUBLIC.TAG.NAME[String(10)]!", columns.get(0).toString());

    assertEquals("BORN", columns.get(3).getName());
    assertTrue(columns.get(3).isDate());
    assertFalse(columns.get(3).isTimestamp());

    assertEquals("ALTERED", columns.get(4).getName());
    assertTrue(columns.get(4).isDate());
    assertTrue(columns.get(4).isTimestamp());

    // System.out.println(columns);
  }

  /** test missing table */
  @Test
  public void testMissingTable() {
    try {
      MetaTable.getMetaData(createMemoryDataSource(), "nosuchtable");
      fail("Should not be able to access table \"nosuchtable\".");
    } catch (TableNotFoundException tex) {
      // that is the place to be
      assertEquals("No such table \"nosuchtable\".", tex.getMessage());
    }
  }

  /**
   * test if we can count records
   *
   * @throws SQLException if table cannot be accessed
   */
  @Test
  public void testCount()
    throws SQLException
  {
    assertEquals(0, QueryUtils.countRows(createMemoryDataSource(), "tag"));

    QueryRunner query = new QueryRunner(createMemoryDataSource());
    query.update("insert into tag (name, altered) values ('alice', sysdate)");
    query.update("insert into tag (name, altered) values ('bob', sysdate)");
    assertEquals(2, QueryUtils.countRows(createMemoryDataSource(), "tag"));
  }

  /**
   * test if foreign keys are listed correctly
   *
   * @throws SQLException if tables cannot be accessed
   */
  @Test
  public void testForeignKeys()
    throws SQLException
  {
    QueryRunner query = new QueryRunner(createMemoryDataSource());
    query.update("create table tagslave ( id integer not null, slave varchar(10) not null, PRIMARY KEY(id) )");
    query.update("alter table tagslave add constraint masterslave foreign key (slave) references tag (name)");

    Set<String> tables = MetaTable.dependsOn(query.getDataSource(), "tag");
    assertNotNull(tables);
    assertEquals(0, tables.size());

    tables = MetaTable.dependsOn(query.getDataSource(), "tagslave");
    assertNotNull(tables);
    assertEquals(1, tables.size());
    assertEquals("TAG", tables.iterator().next());

    tables = MetaTable.dependsMandatoryOn(query.getDataSource(), "tagslave");
    assertNotNull(tables);
    assertEquals(1, tables.size());
    assertEquals("TAG", tables.iterator().next());

    query.update("drop table tagslave");
    query.update("create table tagslave ( id integer not null, slave varchar(10), PRIMARY KEY(id) )");
    query.update("alter table tagslave add constraint masterslave foreign key (slave) references tag (name)");
    tables = MetaTable.dependsMandatoryOn(query.getDataSource(), "tagslave");
    assertNotNull(tables);
    assertEquals(0, tables.size());

    query.update("drop table tagslave");
  }
}
