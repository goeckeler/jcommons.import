package org.jcommons.io.sheet;

import java.util.*;

import org.apache.commons.lang.StringUtils;

/**
 * A book can have a name and contains multiple sheets.
 *
 * Think of it either as an Excel file whereas each Excel tab is a sheet, or as an collection of text files belonging
 * together.
 *
 * With regards to database imports the book is your subset of the database you want to import, e.g. for JUnit tests.
 */
public class Book
{
  private String name;
  private List<Sheet> sheets;

  /** @return the optional name of this book, never null */
  public String getName() {
    return StringUtils.defaultString(name);
  }

  /**
   * Give this book a describing name
   *
   * @param name the name to set
   * @return this to allow chaining
   */
  public Book setName(final String name) {
    this.name = name;
    return this;
  }

  /**
   * Return a single sheet of the book identified by its name
   *
   * @param name the unique name of the sheet
   * @return the sheet identified by the given name, null if there is no such sheet
   */
  public Sheet getSheet(final String name) {
    if (name == null) return null;

    for (Sheet sheet : getSheets()) {
      if (name.equalsIgnoreCase(sheet.getName())) { return sheet; }
    }

    return null;
  }

  /** @return all sheets of this book, never null */
  public List<Sheet> getSheets() {
    if (sheets == null) return Collections.emptyList();
    return sheets;
  }

  /**
   * Place all sheets in this book in one go, will replace existing ones
   *
   * @param sheets the sheets of this book
   * @return this to allow chaining
   */
  public Book setSheets(final List<Sheet> sheets) {
    this.sheets = sheets;
    return this;
  }

  /**
   * Add the given sheets to this book
   *
   * @param sheets the additional sheets of this book
   * @return this to allow chaining
   */
  public Book addAll(final List<Sheet> sheets) {
    if (sheets != null) {
      if (this.sheets == null) {
        this.sheets = sheets;
      } else {
        this.sheets.addAll(sheets);
      }
    }

    return this;
  }

  /**
   * Add the given sheet to this book
   *
   * @param sheet the additional sheet of this book
   * @return this to allow chaining
   */
  public Book add(final Sheet sheet) {
    if (sheet != null) {
      if (this.sheets == null) {
        this.sheets = new ArrayList<Sheet>();
      }
      this.sheets.add(sheet);
    }

    return this;
  }

  /**
   * Remove the given sheets from this book
   *
   * @param sheets the sheets to rip out of this book
   * @return this to allow chaining
   */
  public Book removeAll(final List<Sheet> sheets) {
    if (sheets != null && this.sheets != null) {
      this.sheets.removeAll(sheets);
    }

    return this;
  }

  /**
   * Remove the given sheet from this book
   *
   * @param sheet the sheet to rip out of this book
   * @return this to allow chaining
   */
  public Book remove(final Sheet sheet) {
    if (sheet != null && this.sheets != null) {
      this.sheets.remove(sheet);
    }

    return this;
  }
}
