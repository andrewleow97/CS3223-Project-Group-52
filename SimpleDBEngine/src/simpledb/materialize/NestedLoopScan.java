package simpledb.materialize;

import simpledb.query.*;

/**
 * The Scan class for the <i>nestedloopjoin</i> operator.
 * @author Edward Sciore
 */
public class NestedLoopScan implements Scan {
   private Scan s1;
   private Scan s2;
   private String fldname1, fldname2, opr;
   private Constant joinval = null;
   boolean hasmore1, hasmore2;
   
   /**
    * Create a nestedloop scan for the two underlying sorted scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
   public NestedLoopScan(Scan s1, Scan s2, String fldname1, String fldname2, String opr) {
      this.s1 = s1;
      this.s2 = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.opr = opr;
      beforeFirst();
   }
   
   /**
    * Close the scan by closing the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }
   
  /**
    * Position the scan before the first record,
    * by positioning each underlying scan before
    * their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s1.beforeFirst();
      s2.beforeFirst();
      hasmore1 = s1.next();
	  hasmore2 = s2.next();
   }
   
   public boolean joinCondition(Constant v1, Constant v2, String opr) {
	   switch(this.opr) {
	      case "=":
	    	  return v1.compareTo(v2) == 0;
	      case "<":
	    	  return v1.compareTo(v2) < 0;
	      case "<=":
	    	  return v1.compareTo(v2) < 0 || v1.compareTo(v2) == 0;
	      case ">":
	    	  return v1.compareTo(v2) > 0;
	      case ">=":
	    	  return v1.compareTo(v2) > 0 || v1.compareTo(v2) == 0;
	      case "!=":
	    	  return v1.compareTo(v2) != 0;
	      case "<>":
	    	  return v1.compareTo(v2) != 0;
		  default:
			  return false; 
	   }
   }
   
   /**
    * Move to the next record.  This is where the action is.
    * <P>
    * If the next RHS record has the same join value,
    * then move to it.
    * Otherwise, if the next LHS record has the same join value,
    * then reposition the RHS scan back to the first record
    * having that join value.
    * Otherwise, repeatedly move the scan until a common join value is found.
    * When one of the scans runs out of records, return false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
//      boolean hasmore2 = s2.next();
//      if (hasmore2 && joinval != null && s2.getVal(fldname2).equals(joinval)) {
//    	  hasmore2 = s2.next();
//         return true;
//      }
//      
////      boolean hasmore1 = s1.next();
//      if (hasmore1 && joinval != null && s1.getVal(fldname1).equals(joinval)) {
//    	  hasmore1 = s1.next();
//         return true;
//      }
//      
//      while (hasmore1) {
//    	  Constant v1 = s1.getVal(fldname1);
//    	  while (hasmore2) {
//    		  Constant v2 = s2.getVal(fldname2);
//    		  System.out.println("val1 " + v1 + " val2 " + v2);
//    		  if (joinCondition(v1, v2, this.opr)) {
//    	            joinval = s2.getVal(fldname2);
//    		  		return true;
//    		  }
//    		  else {
//    			  hasmore2 = s2.next();
//    		  }
//    	  }
//    	  s2.beforeFirst();
//    	  hasmore2 = s2.next(); // back to first record
//    	  hasmore1 = s1.next();
//    	  
//      }
//      return false;
	   
	   if (!hasmore1 && !hasmore2) {
		   return false;
	   }
	   
	   while (hasmore1) {
           Constant v1 = s1.getVal(fldname1);
           while (hasmore2 = s2.next()) {
               Constant v2 = s2.getVal(fldname2);
    		  if (joinCondition(v1, v2, this.opr)) {
                  return true;
              } else {
              }
           }
           s2.beforeFirst();
           hasmore1 = s1.next();
       }
	   return false;
   }
   
   /** 
    * Return the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      if (s1.hasField(fldname))
         return s1.getInt(fldname);
      else
         return s2.getInt(fldname);
   }
   
   /** 
    * Return the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      if (s1.hasField(fldname))
         return s1.getString(fldname);
      else
         return s2.getString(fldname);
   }
   
   /** 
    * Return the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (s1.hasField(fldname))
         return s1.getVal(fldname);
      else
         return s2.getVal(fldname);
   }
   
   /**
    * Return true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }
}

