package org.jcommons.db.column;

/**
 * Types that we can handle in both the import sources and the database
 *
 * @author Thorsten Goeckeler
 */
public enum MetaType
{
  /** a date type without time part */
  DATE,
  /** a numeric type */
  NUMBER,
  /** a string type */
  STRING,
  /** a date type with time part */
  TIMESTAMP;
}
