package org.jcommons.db.load;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jcommons.io.sheet.Book;
import org.jcommons.io.sheet.Sheet;


/**
 * Merges a book into the given data source.
 *
 * Tables and columns must exist and will not be created!
 *
 * @author Thorsten Goeckeler
 */
public class DatabaseLoader
{
  private static final Log LOG = LogFactory.getLog(DatabaseLoader.class);

  private DataSource dataSource;

   /** @return the currently used data source */
  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * Inject the data source to be used to load the data.
   *
   * @param dataSource the database connection to use to load the data
   * @return this to allow chaining
   */
  public DatabaseLoader setDataSource(final DataSource dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  /**
   * Load the given book into the given database, either insert or update the data.
   *
   * @param book the data set to load into the database
   * @throws SQLException if load cannot be performed
   */
  public void load(final Book book)
    throws SQLException
  {
    if (book == null) return;

    if (getDataSource() == null) {
      StringBuilder log = new StringBuilder("Cannot import book ").append(defaultName(book));
      log.append("as no database connection can be established.");
      LOG.error(log.toString());
      return;
    }

    if (LOG.isInfoEnabled()) {
      StringBuilder log = new StringBuilder("Importing book ").append(defaultName(book));
      log.append("with ").append(book.getSheets().size()).append(" sheets into the database.");
      LOG.info(log.toString());
    }

    // simple load strategy, load every sheet in one go, but all sheets in the same transaction
    SheetLoader loader = new SheetLoader().setDataSource(getDataSource());
    for (Sheet sheet : getSheets(book)) {
      loader.load(sheet);
    }

    if (LOG.isInfoEnabled()) {
      StringBuilder log = new StringBuilder("Imported book ").append(defaultName(book));
      log.append("with ").append(book.getSheets().size()).append(" sheets into the database.");
      LOG.info(log.toString());
    }
  }

  /**
   * Return the sheets in the order they shall be loaded.
   *
   * @param book the book containing the sheets, never null
   * @return the ordered list of sheets
   */
  protected List<Sheet> getSheets(final Book book) {
    // for the time being return in same order as given in the sheet
    return book.getSheets();
  }

  /**
   * Determine the name of the book for debug messages
   *
   * @param book the currently inspected book
   * @return the quoted name of the book or the empty string if it has no name
   */
  private String defaultName(final Book book) {
    StringBuilder text = new StringBuilder();
    if (book != null && StringUtils.isNotBlank(book.getName())) {
      text.append("\"").append(book.getName()).append("\" ");
    }

    return text.toString();
  }
}
