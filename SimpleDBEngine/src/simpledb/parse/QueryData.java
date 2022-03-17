package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
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
   private List<AggregationFn> aggFields;
   private List<String> groupList;
   private boolean isDistinct;
   
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
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, 
		   List<List<String>> sortFields, List<AggregationFn> aggFields, List<String> groupList, boolean isDistinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;

      if (sortFields != null) {

	      this.sortFields = sortFields.get(0);
	      this.sortOrder = sortFields.get(1);
      }
      if (aggFields.size() > 0) {
    	  this.aggFields = aggFields;
      }
      if (groupList != null) {
    	  this.groupList = groupList;
      }
      this.isDistinct = isDistinct;
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
   
   /**
    * Returns the list of field names to be ordered.
    * @return the list of field names to be ordered.
    */
   public List<String> sortFields() {
	   return sortFields;
   }
   
   /**
    * Returns the list of order type.
    * @return the list of order type.
    */
   public List<String> sortOrder() {
	   return sortOrder;
   }
   
   
   /**
    * Returns the list of aggregates functions.
    * @return the list of aggregate functions.
    */
   public List<AggregationFn> aggFields() {
	   return aggFields;
   }
   
   /**
    * Returns the list of fields to be group by.
    * @return the list of fields to be group by.
    */
   public List<String> groupList() {
	   return groupList;
   }
   
   
   /**
    * Returns true if output require to be distinct, else false.
    * @return true if output require to be distinct, else false.
    */
   public boolean isDistinct() {
	   return isDistinct;
   }
   
   /**
    * Returns SQL Query (not use at the moment)
    * @return SQL Query
    */
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
      if (sortFields != null) {
    	  result += "order by ";
    	  for (int i = 0; i < sortFields.size(); i++) {
    		  result += sortFields.get(i) + " ";
    		  result += sortOrder.get(i) + ", ";
    	  }
    	  result = result.substring(0, result.length()-2); //remove final comma
      }
    	  
      return result;
   }
}
