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
 * The Scan class for the <i>mergejoin</i> operator.
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
	private List<Integer> keys;
	private HashMap<Integer, TempTable> p1;
	private HashMap<Integer, TempTable> p2;
	private Schema sch;

	/**
	 * Create a mergejoin scan for the two underlying sorted scans.
	 * 
	 * @param s1       the LHS sorted scan
	 * @param s2       the RHS sorted scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */
	public HashJoinScan(Transaction tx, HashMap<Integer, TempTable> p1, HashMap<Integer, TempTable> p2, String fldname1,
			String fldname2, Schema sch) {
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.tx = tx;
		this.hashval = tx.availableBuffs();
		this.keyIndex = 0;
		this.keys = new ArrayList<>(p1.keySet());
		this.sch = sch;
		this.p1 = p1;
		this.p2 = p2;
		h1 = new HashMap<>();
		for (int i = 0; i < hashval; i++) {
			TempTable currenttemp = new TempTable(tx, sch);
			h1.put(i, currenttemp);
		}
		rehash();
		// rehashing p1 into h1 by scanning partition p1 and adding its vals to
		// temptables in h1 by new hash
		this.keyIndex = 0;
		this.s2 = (UpdateScan) p2.get(this.keyIndex).open(); // starting at bucket 0
		beforeFirst();
	}

	
	public void rehash() {
		while (this.keyIndex <= hashval) {
	        int key = this.keys.get(this.keyIndex);
	        int hash1 = 0;
	        // getting temptable from partition 1 in hashtable
	        TempTable p1 = this.p1.get(key);
	        UpdateScan tempscan = p1.open();
	        tempscan.beforeFirst();

	        boolean hasmore = tempscan.next();	        
	        if (!hasmore) {
	            this.keyIndex += 1;
	            tempscan.close();
	            continue;
	        }
	        while (hasmore) {
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
	            hasmore = tempscan.next();
	        }
	        tempscan.close();
	        this.keyIndex += 1;
	    }
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
	 * Position the scan before the first record, by positioning each underlying
	 * scan before their first records.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s2.beforeFirst();
	}

	public void savePosition() {
		RID rid1 = (s1 == null) ? null : s1.getRid();
		RID rid2 = (s2 == null) ? null : s2.getRid();
		savedposition = Arrays.asList(rid1, rid2);
	}

	/**
	 * Move the scan to its previously-saved position.
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
	    /**
	     * 1. TAKE IN ENTIRE REHASHED HASHMAP OF S1 AND SCAN S2 OF PARTITION K OF S2 2.
	     * FOR EACH VALUE OF S2, REHASH IT, CHECK IF HASH VALUE IN HASHMAP OF S1 3. IF
	     * MATCH, OPEN SCAN ON HASHMAP(KEY) 4. JOIN BASED ON FLDNAME1 AND FLDNAME2,
	     * RETURN TRUE + SAVE POSITION IF MATCH ELSE INCREMENT S1.NEXT() 5. WHEN
	     * S1.NEXT() IS NULL, S2.NEXT() 6. WHEN S2.NEXT() IS NULL RETURN FALSE
	     */
	    while (this.keyIndex <= hashval) {
	        if (savedposition != null) {
	            restorePosition();
	        } else {
	            s2.beforeFirst();
	        }
	        boolean hasmore2 = this.s2.next();
	        int hash2 = 0;
	        if (!hasmore2) {
	            this.keyIndex += 1;
	            this.s2.close();
		        if (this.keyIndex <= hashval) {
		        	this.s2 = (UpdateScan) this.p2.get(this.keyIndex).open();
		        	this.s2.beforeFirst();
		        }
		        this.savedposition = null;
	            continue;
	        }
	        while (hasmore2) {
	            try {
	                int joinval2 = s2.getInt(fldname2);
	                hash2 = joinval2 % hashval;
	    
	            } catch (NumberFormatException e) { // not an int
	                String joinval2 = s2.getString(fldname2);
	                hash2 = ((joinval2 == null) ? 0 : joinval2.hashCode()) % hashval;
	    
	            }
	            
	            if (h1.containsKey(hash2)) {
	                this.s1 = h1.get(hash2).open();
	                this.s1.beforeFirst();
	                boolean hasmore1 = s1.next();
	                if (!hasmore1) {
	                    s1.close();
	                    continue;
	                }
	                while (hasmore1) {
	                    if (this.s1.getVal(fldname1).compareTo(this.s2.getVal(fldname2)) == 0) { // match on joinval
//	                        // need to copy in the values
	                    	for (String field : this.p2.get(this.keyIndex).getLayout().schema().fields()) {
	                    		this.s1.setVal(field, this.s2.getVal(field));
	                    	}

	                        savePosition();
	                        return true;
	                    }
	                    hasmore1 = s1.next();
	                }
	                this.s1.close();
	            }
	            hasmore2 = s2.next();
	        }
	        this.s2.close();
	        this.savedposition = null;
	        this.keyIndex += 1;
	        if (this.keyIndex <= hashval) {
	        	this.s2 = (UpdateScan) this.p2.get(this.keyIndex).open();
	        	this.s2.beforeFirst();
	        	
	        }
	    }
	    close();
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