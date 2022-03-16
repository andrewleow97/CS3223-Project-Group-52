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
//import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * The Scan class for the merging phase of the <i>hashjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinScan implements Scan {
	private UpdateScan s1;
	private UpdateScan s2;
	private String fldname1, fldname2;
	private Transaction tx;
	private List<RID> savedposition = null;
	private int hashval = 0;
	private HashMap<Integer, TempTable> h1;
	private int keyIndex = 0;
	private HashMap<Integer, TempTable> p1;
	private HashMap<Integer, TempTable> p2;
	private Schema sch;

	/**
	 * Create a hashjoin scan for the two underlying sorted scans.
	 * 
	 * @param p1       the outer table partitions
	 * @param p2       the inner table partitions
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param tx the calling transaction
	 * @param sch the schema
	 */
	public HashJoinScan(Transaction tx, HashMap<Integer, TempTable> p1, HashMap<Integer, TempTable> p2, String fldname1,
			String fldname2, Schema sch) {
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.tx = tx;
		this.hashval = tx.availableBuffs() - 2; // new hash value is B - 2
		this.keyIndex = 0;
		this.sch = sch;
		this.p1 = p1;
		this.p2 = p2;
		h1 = new HashMap<>();
		
		// create empty TempTables for each hash value and initialize into h1
		for (int i = 0; i < hashval; i++) {
			TempTable currenttemp = new TempTable(tx, sch);
			h1.put(i, currenttemp);
		}
		
		// starting with first bucket
		this.keyIndex = 0;
		// rehash first bucket of p1 into h1
		rehash();
		// open scan on p2 starting at bucket 0
		this.s2 = (UpdateScan) p2.get(this.keyIndex).open(); 
		beforeFirst();
	}

	/**
	 * Rehashes a bucket of p1 into a new hash table h1.
	 * Uses a new hash value for rehashing to differentiate the hash functions.
	 * The number of buckets is determined by B, the number of available buffers - 2.
	 * The values are copied from the scan into the TempTable in h1 for all the schema fields.
	 */
	public void rehash() { // rehash partition of p1 per bucket @ this.keyindex

		// getting temptable from partition 1 in hashtable
		TempTable p1 = this.p1.get(this.keyIndex);
		UpdateScan tempscan = p1.open();
		tempscan.beforeFirst();
		boolean hasmore;
		int hash1 = 0;
		while (hasmore = tempscan.next()) { // while there are values in this partition
			try {
				int joinval1 = tempscan.getInt(fldname1);
				hash1 = joinval1 % hashval;

			} catch (NumberFormatException e) { // not an int
				String joinval1 = tempscan.getString(fldname1);
				hash1 = ((joinval1 == null) ? 0 : joinval1.hashCode()) % hashval;

			}

			// transferring all data @ current tempscan of p1 into h1
			UpdateScan currscan = h1.get(hash1).open();
			currscan.insert();
			for (String fldname : p1.getLayout().schema().fields()) {
				currscan.setVal(fldname, tempscan.getVal(fldname));

			}
			currscan.close();
		}
		tempscan.close();
		return;
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
	 * Position the scan before the first record in s2.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s2.beforeFirst();
	}

	/**
	 * Saves the positions of scans s1 and s2 by saving their Rid using the getRid() function
	 * These positions will be restored when the next() function is called again to resume progress
	 */
	public void savePosition() {
		RID rid1 = (s1 == null) ? null : s1.getRid();
		RID rid2 = (s2 == null) ? null : s2.getRid();
		savedposition = Arrays.asList(rid1, rid2);
	}

	/**
	 * Move the scan to its previously-saved position using the moveToRid() function.
	 */
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
	 * Performs the merging operation for all buckets based on the current hash value.
	 * If the value in the same bucket of h1 matches the rehashed value of the current tuple of s2, 
	 * copy the value of s2's tuple into h1 and return true.
	 * 
	 * Close and reopen the scan of s2, and rehash the next partition of s1 after completion of each hash partition.
	 * When the scan on s2 completes, return false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */

	public boolean next() {
		/**
		 * Method used:
		 * 
		 * 1. TAKE IN ENTIRE REHASHED HASHMAP OF S1 AND SCAN S2 OF PARTITION K OF S2 
		 * 2.FOR EACH VALUE OF S2, REHASH IT, CHECK IF HASH VALUE IN HASHMAP OF S1 
		 * 3. IF MATCH, OPEN SCAN ON HASHMAP(KEY) 
		 * 4. JOIN BASED ON FLDNAME1 AND FLDNAME2, RETURN TRUE + SAVE POSITION IF MATCH ELSE INCREMENT S1.NEXT() 
		 * 5. WHEN S1.NEXT() IS NULL, S2.NEXT() 
		 * 6. WHEN S2.NEXT() IS NULL RETURN FALSE (COMPLETED SCAN OF 2ND TABLE)
		 */

		while (this.keyIndex <= hashval) { // starting is bucket 0 of p2 
			if (savedposition != null) { // return to position and complete joining on the same value
	            restorePosition();
				int hash2 = 0;
				try {
					int joinval2 = this.s2.getInt(fldname2);
					hash2 = joinval2 % hashval;

				} catch (NumberFormatException e) { // not an int
					String joinval2 = this.s2.getString(fldname2);
					hash2 = ((joinval2 == null) ? 0 : joinval2.hashCode()) % hashval;

				}
				
				while (this.s1.next()) {
                    if (this.s1.getVal(fldname1).compareTo(this.s2.getVal(fldname2)) == 0) { // match on joinval
                        // need to copy in the values
                    	for (String field : this.p2.get(this.keyIndex).getLayout().schema().fields()) {
                    		this.s1.setVal(field, this.s2.getVal(field));
                    	}
                        savePosition(); // save and restore again
                        return true;

                    }
				}
				this.s1.close(); // completed partition of s1
	        }
			
			boolean hasmore2;
			while (hasmore2 = this.s2.next()) { // while there are more tuples in s2
				int hash2 = 0;
				try {
					int joinval2 = this.s2.getInt(fldname2);
					hash2 = joinval2 % hashval;

				} catch (NumberFormatException e) { // not an int
					String joinval2 = this.s2.getString(fldname2);
					hash2 = ((joinval2 == null) ? 0 : joinval2.hashCode()) % hashval;

				}
				// open scan on temptable of hash2 in h1
				this.s1 = h1.get(hash2).open();
				this.s1.beforeFirst();
				boolean hasmore1;
				while (hasmore1 = this.s1.next()) {
                    if (this.s1.getVal(fldname1).compareTo(this.s2.getVal(fldname2)) == 0) { // match on joinval
                        // need to copy in the values
                    	for (String field : this.p2.get(this.keyIndex).getLayout().schema().fields()) {
                    		this.s1.setVal(field, this.s2.getVal(field));
                    	}
                        savePosition();
                        return true;

                    }
				}
				this.s1.close();
			}
			this.s2.close();
			//clear h1
			this.h1.clear();
			for (int i = 0; i < hashval; i++) {
				TempTable currenttemp = new TempTable(tx, sch);
				h1.put(i, currenttemp);
			}
			// move to next bucket
			this.keyIndex += 1;
			if (keyIndex > hashval) { // no more buckets to join
				close();
				return false;
			}
			// reset saved position, and rehash and reopen scans on both tables
			this.savedposition = null;
			rehash();
			this.s2 = (UpdateScan) p2.get(this.keyIndex).open();
			this.s2.beforeFirst();
		}
		close();
		return false;
	}

	/**
	 * Return the integer value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 * @return returns the integer value of the specified field in either underlying scan
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
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 * @return returns the String value of the specified field in either underlying scan
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
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 * @return returns the value of the specified field in either underlying scan
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
	 * @param fldname the field name
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 * @return returns if either field contains the fieldname
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}
}