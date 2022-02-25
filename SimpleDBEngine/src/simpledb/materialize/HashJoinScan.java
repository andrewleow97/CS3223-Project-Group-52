package simpledb.materialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinScan implements Scan {
	private UpdateScan s1;
	private UpdateScan s2;
	private String fldname1, fldname2;
	private Transaction tx;
	private RID savedposition;
	private int hashjoinval = 0;
	private int hashval = 0;

	/**
	 * Create a mergejoin scan for the two underlying sorted scans.
	 * 
	 * @param s1       the LHS sorted scan
	 * @param s2       the RHS sorted scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */
	public HashJoinScan(Transaction tx, UpdateScan s1, UpdateScan s2, String fldname1, String fldname2) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.tx = tx;
		this.hashval = (int) Math.ceil(Math.sqrt(tx.availableBuffs())) - 2;
		s1.beforeFirst();
		s2.beforeFirst();

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
	}

	public void savePosition() {
		RID rid2 = (s2 == null) ? null : s2.getRid();
		savedposition = rid2;
	}

	/**
	 * Move the scan to its previously-saved position.
	 */
	public void restorePosition() {
		if (savedposition != null)
			s2.moveToRid(savedposition);
	}

	/**
	 * Move to the next record. This is where the action is.
	 * <P>
	 * If the next RHS record has the same join value, then move to it. Otherwise,
	 * if the next LHS record has the same join value, then reposition the RHS scan
	 * back to the first record having that join value. Otherwise, repeatedly move
	 * the scan having the smallest value until a common join value is found. When
	 * one of the scans runs out of records, return false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */

	public boolean next() {
		// ONLY DOING SCANNING, REHASHING OF S2
		beforeFirst();
		restorePosition();
		boolean hasmore2 = s2.next();
		if (hasmore2 && s2.getVal(fldname2).equals(hashjoinval))
	         return true;
		boolean hasmore1 = s1.next();
		
		int hash1 = 0;
		int hash2 = 0;
		while (hasmore1 && hasmore2) {
			// hash table = partition of s1 
			// temp table of new hashtable of s1 (<10, r1>, <10, r2>, r3) (<10, s1>)
			try {
				int joinval1 = s1.getInt(fldname1);
				hash1 = joinval1 % hashval;
				int joinval2 = s2.getInt(fldname2);
				hash2 = joinval2 % hashval;

			} catch (NumberFormatException e) { // not an int
				String joinval1 = s1.getString(fldname1);
				hash1 = ((joinval1 == null) ? 0 : joinval1.hashCode()) % hashval;
				String joinval2 = s2.getString(fldname2);
				hash2 = ((joinval2 == null) ? 0 : joinval2.hashCode()) % hashval;

			}

			if (hash1 == hash2) {
				// while hasmore1 && hash1 == hash2
				// pause scan here or smth
				hashjoinval = hash1;
				savePosition();
				return true;
			}

		}
		return false;
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