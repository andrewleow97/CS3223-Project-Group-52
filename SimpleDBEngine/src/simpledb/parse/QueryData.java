package simpledb.parse;

import java.util.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> sortFields;
   private List<String> sortOrder;
   
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
	      this.fields = fields;
	      this.tables = tables;
	      this.pred = pred;
	      this.sortFields = null;
	   }
   
   /**
    * Saves the field and table list and predicate with sorting.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<List<String>> sortFields) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortFields = sortFields.get(0);
      this.sortOrder = sortFields.get(1);
      
   }
   
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public List<String> sortFields() {
	   return sortFields;
   }
   
   public List<String> sortOrder() {
	   return sortOrder;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
//      if (sortFields != null) {
//       if (!sortFields.isEmpty()) {
//    	  result += "order by ";
//    	  for (List<String> i : sortFields) { //majorid, asc
//    		  for (String j : i) {
//    			  result += j + " ";
//    		  }
//    		  result += ", ";
//    	  }
//       }	  
//      }
    	  
      return result;
   }
}
