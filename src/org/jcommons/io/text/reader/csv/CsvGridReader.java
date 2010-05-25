package org.jcommons.io.text.reader.csv;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrMatcher;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jcommons.io.text.Grid;
import org.jcommons.io.text.files.Files;
import org.jcommons.io.text.reader.GridReader;


/**
 * Reads comma separated text files to fill in a table structure
 *
 * @author Thorsten Goeckeler
 */
public class CsvGridReader
  implements GridReader
{
  private static final Log LOG = LogFactory.getLog(CsvGridReader.class);

  private static final String DEFAULT_DELIMITER = ",";
  private static final String DEFAULT_ESCAPE = "\"";

  private String delimiter = DEFAULT_DELIMITER;
  private String escape = DEFAULT_ESCAPE;

  private File file;
  private StrTokenizer tokenizer;

  /** Create a reader, specify the file later. */
  public CsvGridReader() {
    file = null;
  }

  /**
   * Construct a reader for the given file name.
   *
   * @param fileName the path to the file which should be read
   */
  public CsvGridReader(final String fileName) {
    file = new File(fileName);
  }

  /**
   * Construct a reader for the given file.
   *
   * @param file the file to read from
   */
  public CsvGridReader(final File file) {
    this.file = file;
  }

  /**
   * Specify which file to read from
   *
   * @param file the file to read from
   * @return this to allow chaining
   */
  public CsvGridReader setFile(final File file) {
    this.file = file;
    return this;
  }

  /** @return the current file we want to read from */
  public File getFile() {
    return file;
  }

  /**
   * Reads the given file, which is interpreted as a CSV file, and creates the table structure.
   *
   * @return the table structure that resembles the file layout
   */
  @Override
  public Grid read() {
    if (file == null) return null;

    LOG.info(new StringBuilder("Reading from text file \"").append(file.getAbsolutePath()).append("\".").toString());
    LineNumberReader reader = Files.open(file);
    if (reader == null) return null;

    Grid table = new Grid();
    try {
      String row = null;
      while ((row = reader.readLine()) != null) {
        table.add(toRow(row));
      }
      if (LOG.isInfoEnabled()) {
        StringBuilder log = new StringBuilder("Completed reading from text file \"");
        log.append(file.getAbsolutePath()).append("\".");
        LOG.info(log.toString());
      }
    } catch (IOException ioex) {
      StringBuilder log = new StringBuilder("Aborted reading from text file \"").append(file.getAbsolutePath());
      log.append(":").append(reader.getLineNumber()).append("\".");
      LOG.warn(log.toString(), ioex);
      table = null;
    } finally {
      Files.close(reader, file);
      reader = null;
    }

    return table;
  }

  /**
   * splits a line in columns
   *
   * @param data a single line as read from the file
   * @return the separated strings
   */
  List<String> toRow(final String data) {
    List<String> row = new ArrayList<String>();
    if (StringUtils.isBlank(data)) return row;

    // if you simply use data.split() you cannot use escape characters
    // however it is very kind of Apache to have a solution for that
    getTokenizer().reset(data);
    while (getTokenizer().hasNext()) {
      row.add(getTokenizer().nextToken());
    }
    return row;
  }

  /** @return the current delimiter that determines how to separate fields from each other */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * Define which character sequence shall be interpreted to separate the columns
   *
   * @param delimiter the delimiter to be used, <code>null</code> to reset to the default delimiter
   * @return this to allow chaining
   */
  public CsvGridReader setDelimiter(final String delimiter) {
    this.delimiter = StringUtils.defaultIfEmpty(delimiter, DEFAULT_DELIMITER);
    this.tokenizer = null;
    return this;
  }

  /** @return the optional character sequence that escapes the delimiter in string values */
  public String getEscape() {
    return escape;
  }

  /**
   * Define which character encloses strings to escape delimiters contained in that string
   *
   * The escape character is optional and will not be part of the input. If you want to indicate that strings are never
   * escaped, than use the empty string.
   *
   * @param escape the escape character sequence to be used, <code>null</code> to reset to the default
   * @return this to allow chaining
   */
  public CsvGridReader setEscape(final String escape) {
    this.escape = StringUtils.defaultIfEmpty(escape, DEFAULT_ESCAPE);
    this.tokenizer = null;
    return this;
  }

  /** @return the row tokenizer to convert a line into columns */
  private StrTokenizer getTokenizer() {
    if (tokenizer == null) {
      // separate using the given delimiter
      tokenizer = new StrTokenizer("", getDelimiter());
      // allow quoting (escaping the delimiter) - the quote is escaped by double quoting
      tokenizer.setQuoteMatcher(StrMatcher.charSetMatcher(getEscape()));
      // removed leading and trailing spaces
      tokenizer.setTrimmerMatcher(StrMatcher.trimMatcher());
      // automatically ignore empty columns at the end, we don't want that!
      tokenizer.setIgnoreEmptyTokens(false);
      tokenizer.setEmptyTokenAsNull(true);
    }

    return tokenizer;
  }
}
