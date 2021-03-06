package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<String> fields;
   private List<String> sortFields;
   private List<String> sortOrder;
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public RecordComparator(List<String> fields) {
      this.fields = fields;
   }
   
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param sortFields a list of field names
    * @param sortOrder a list of sorting order directions
    */
   public RecordComparator(List<String> sortFields, List<String> sortOrder) {
	      this.sortFields = sortFields;
	      this.sortOrder = sortOrder;
   }
   
   /**
    * Compare the current records of the two specified scans.
    * If sort order is specified, consider the sort order and sort fields in order.
    * Else the sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
	   if (this.sortOrder != null) { // if sorting order is specified
		   for (int i = 0; i < sortFields.size(); i++) {
			   Constant val1 = s1.getVal(sortFields.get(i));
			   Constant val2 = s2.getVal(sortFields.get(i));
			   int result = val1.compareTo(val2);
			   if (result != 0) {
				   if (sortOrder.get(i) == "desc") { // flip sorting result if desc is specified
				   result *= -1;
				   }
			   return result;
			   }
		   }
	   return 0;
	  } else { // if sorting order is not specified

	      for (String fldname : fields) { 
	         Constant val1 = s1.getVal(fldname); 
	         Constant val2 = s2.getVal(fldname);
	         int result = val1.compareTo(val2);
	         if (result != 0)
	            return result;
	      }
      return 0;
	  }
   }
}
