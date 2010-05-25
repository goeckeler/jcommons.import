package org.jcommons.io.text.files;

import java.io.*;

import javax.annotation.processing.Filer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convenience functions to access text files easily.
 *
 * @author Thorsten Goeckeler
 */
public final class Files
{
  private static final Log LOG = LogFactory.getLog(Filer.class);

  /** hide default constructor */
  private Files() {
  }

  /**
   * Create a new file with the given name
   *
   * @param fileName the path describing the file to create, never null
   * @return the created buffered writer or null if an error occurred which will be logged
   */
  public static BufferedWriter create(final String fileName) {
    if (StringUtils.isBlank(fileName)) return null;
    BufferedWriter writer = null;

    try {
      writer = new BufferedWriter(new FileWriter(fileName));
    } catch (IOException ioex) {
      writer = null;
      if (LOG.isWarnEnabled()) {
        LOG.warn(new StringBuilder("Cannot create file \"").append(fileName).append("\".").toString(), ioex);
      }
    }

    return writer;
  }

  /**
   * Opens a file gracefully
   *
   * @param file the file to open, never null
   * @return the corresponding reader or null if the file cannot be opened
   */
  public static LineNumberReader open(final File file) {
    if (file == null) return null;

    if (!file.exists()) {
      if (LOG.isWarnEnabled()) {
        StringBuilder log = new StringBuilder("Cannot open file \"").append(file.getAbsolutePath());
        log.append("\" as there is no such file.");
        LOG.warn(log.toString());
      }

      return null;
    }

    if (!file.canRead()) {
      if (LOG.isWarnEnabled()) {
        StringBuilder log = new StringBuilder("Cannot open file \"").append(file.getAbsolutePath());
        log.append("\" as I am not allowed to read it.");
        LOG.warn(log.toString());
      }

      return null;
    }

    LineNumberReader reader = null;
    try {
      reader = new LineNumberReader(new FileReader(file));
      // start counting with 1 as that is what we expect for files
      reader.setLineNumber(1);
    } catch (IOException ioex) {
      reader = null;
      LOG.warn(new StringBuilder("Cannot open file \"").append(file.getAbsolutePath()).append("\".").toString(), ioex);
    }

    return reader;
  }

  /**
   * Opens a file gracefully
   *
   * @param fileName the path describing the file to open, never null
   * @return the corresponding reader or null if the file cannot be opened
   */
  public static LineNumberReader open(final String fileName) {
    if (StringUtils.isBlank(fileName)) return null;
    File file = new File(fileName);
    return open(file);
  }

  /**
   * Close a stream gracefully, no matter if it is open or not or if cannot be closed for some unknown reason
   *
   * @param stream the stream to close, never null
   */
  public static void close(final Closeable stream) {
    close(stream, null);
  }

  /**
   * Close a stream gracefully, no matter if it is open or not or if cannot be closed for some unknown reason
   *
   * @param stream the stream to close, never null
   * @param file the file that this stream is associated to, optional
   */
  public static void close(final Closeable stream, final File file) {
    if (stream == null) return;
    try {
      stream.close();
    } catch (IOException ioex) {
      StringBuilder log = new StringBuilder("Cannot close file");
      if (file != null) {
        log.append(" \"").append(file.getAbsolutePath()).append("\"");
      }
      log.append(".");
      LOG.debug(log.toString(), ioex);
    }
  }
}
