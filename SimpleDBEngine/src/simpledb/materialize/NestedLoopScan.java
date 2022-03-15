package simpledb.materialize;

import java.util.Arrays;
import java.util.List;

import simpledb.query.*;
import simpledb.record.RID;

/**
 * The Scan class for the <i>nestedloopjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class NestedLoopScan implements Scan {
	private UpdateScan s1;
	private UpdateScan s2;
	private String fldname1, fldname2, opr;
	boolean hasmore1, hasmore2;
	private List<RID> savedposition = null;

	/**
	 * Create a nestedloop scan for the two underlying sorted scans.
	 * 
	 * @param s1       the LHS scan
	 * @param s2       the RHS scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */
	public NestedLoopScan(Scan s1, Scan s2, String fldname1, String fldname2, String opr) {
		this.s1 = (UpdateScan) s1;
		this.s2 = (UpdateScan) s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.opr = opr;
		beforeFirst();
	}

	/**
	 * Close the scan by closing the two underlying scans.
	 * 
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		s2.close();
	}

	/**
	 * Position the scan before the first record, by positioning each underlying
	 * scan before their first records.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
		s2.beforeFirst();
		hasmore1 = s1.next();
		hasmore2 = s2.next();
	}

	public boolean joinCondition(Constant v1, Constant v2, String opr) {
		switch (this.opr) {
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

	public void savePosition() {
		RID rid1 = (s1 == null) ? null : s1.getRid();
		RID rid2 = (s2 == null) ? null : s2.getRid();
		savedposition = Arrays.asList(rid1, rid2);
	}

	public void restorePosition() {
		RID rid1 = savedposition.get(0);
		if (rid1 != null) {
			s1.moveToRid(rid1);
		}
		RID rid2 = savedposition.get(1);
		if (rid2 != null)
			s2.moveToRid(rid2);
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
	   if (!hasmore1 && !hasmore2) {
		   return false;
	   }
	   if (savedposition != null) {
		   restorePosition();
		   hasmore2 = s2.next();
		   while (hasmore2) {
			   Constant v1 = s1.getVal(fldname1);
			   Constant v2 = s2.getVal(fldname2);
			   if (joinCondition(v1, v2, this.opr)) {
	 			  savePosition();
	              return true;
			   } else {
				   break;
			   }
		   }
		   s2.beforeFirst();
		   hasmore2 = s2.next();
		   savedposition = null;
		   hasmore1 = s1.next();
	   }
	   while (hasmore1) {
           Constant v1 = s1.getVal(fldname1);
           while (hasmore2) {
               Constant v2 = s2.getVal(fldname2);
    		  if (joinCondition(v1, v2, this.opr)) {
    			  savePosition();
                  return true;
              }
    		  hasmore2 = s2.next();
           }
           s2.beforeFirst();
           savedposition = null;
           hasmore1 = s1.next();
       }
	   return false;
//	   while (true) {
//		   while (s2.next()) {
//			   
//			   if (s1.getVal(fldname1).equals(s2.getVal(fldname2)))
//				   return true;
//		   }
//		   s2.beforeFirst();
//		   if (!s1.next())
//			   return false;
//	   }
   }

	/**
	 * Return the integer value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (s1.hasField(fldname))
			return s1.getInt(fldname);
		else
			return s2.getInt(fldname);
	}

	/**
	 * Return the string value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		if (s1.hasField(fldname))
			return s1.getString(fldname);
		else
			return s2.getString(fldname);
	}

	/**
	 * Return the value of the specified field. The value is obtained from whichever
	 * scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (s1.hasField(fldname))
			return s1.getVal(fldname);
		else
			return s2.getVal(fldname);
	}

	/**
	 * Return true if the specified field is in either of the underlying scans.
	 * 
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}
}
