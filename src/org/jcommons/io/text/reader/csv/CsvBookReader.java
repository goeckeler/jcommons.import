package org.jcommons.io.text.reader.csv;

import java.io.File;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jcommons.io.sheet.Book;
import org.jcommons.io.sheet.Sheet;
import org.jcommons.io.text.Grid;
import org.jcommons.io.text.Table;
import org.jcommons.io.text.factory.TableFactory;
import org.jcommons.io.text.reader.BookReader;


/**
 * Reads multiple CSV files into a single book.
 *
 * @author Thorsten Goeckeler
 */
public class CsvBookReader
  implements BookReader
{
  private static final Log LOG = LogFactory.getLog(CsvBookReader.class);

  private final List<File> files;
  private final File rootDirectory;
  private final IOFileFilter filter;

  /** Sole constructor to create a reader. */
  public CsvBookReader() {
    files = new ArrayList<File>();
    rootDirectory = null;
    filter = null;
  }

  /**
   * Add a file to be added to the book
   *
   * @param file the file to interpreted as well
   * @return this to allow chaining
   */
  public CsvBookReader addFile(final File file) {
    files.add(file);
    return this;
  }

  /**
   * Add multiple files to be added to the book
   *
   * @param files the files to interpreted as well
   * @return this to allow chaining
   */
  public CsvBookReader addFiles(final Collection<File> files) {
    files.addAll(files);
    return this;
  }

  /**
   * Add a file filter so that all matching files will be added automatically
   *
   * @param rootDirectory the root directory where to search for the files, <code>null</code> to reset
   * @param filter the filter that selects the files to import, <code>null</code> to import none
   * @return this to allow chaining
   */
  public CsvBookReader setFilter(final File rootDirectory, final IOFileFilter filter) {
    return this;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public Book read(final Map<String, String> arguments) {
    Book book = new Book();

    if (rootDirectory != null && filter != null) {
      // add all matching files
      files.addAll(FileUtils.listFiles(rootDirectory, filter, FileFilterUtils.makeCVSAware(null)));
    }

    if (!files.isEmpty()) {
      LOG.info("Loading book from " + files.size() + " files.");

      CsvGridReader reader = new CsvGridReader();
      for (File file : files) {
        Grid grid = reader.setFile(file).read();
        Table table = TableFactory.create(grid, arguments);
        Sheet sheet = new Sheet().setTable(table).setName(FilenameUtils.getBaseName(file.getName()));
        book.add(sheet);
      }

      LOG.info("Loaded book with " + book.getSheets().size() + " sheets.");
    }

    return book;
  }

  /** {@inheritDoc} */
  @Override
  public Book read() {
    return read(null);
  }
}
