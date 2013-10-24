package org.jcommons.db.query;

import java.io.Serializable;

/**
 * A query with named parameters that can be used as a prepared statement.
 *
 * Example query would be
 *
 * <pre>
 *    select name from customer where customer_id = :customer and name like :search
 * </pre>
 *
 * Parameters are <code>customer</code> and <code>search</code>. In this case usage would be as follows:
 *
 * <pre>
 *   QueryString query = new QueryString("select name ...");
 *   QueryRunner db = new QueryRunner(getDataSource());
 *
 *   // easy single row execution
 *   db.query(query.getSql(), query.with("customer", customer).with("search", "%motor%").asRow(), ...);
 *
 *   // mapped single row execution
 *   Map<String, Object> values = ...
 *   values.put("customer", customer);
 *   valued.put("search", "%motor%");
 *   db.query(query.getSql(), query.with(values).asRow(), ...);
 *
 *   // easy multiple row execution

 *   // column names should match parameter names, if not use translation table
 *   Map<String, String> translate = ...
 *   translate.put(columnName, parameterName);
 *   String[] headers = ...
 *   Object[][] values = ...
 *
 *   db.query(query.getSql(), query.header(headers).map(translate).with(values).asArray(), ...);
 * </pre>
 *
 * @author Thorsten Goeckeler
 */
public class QueryString
  implements Serializable
{
  private static final long serialVersionUID = -4350007400588806587L;
}
