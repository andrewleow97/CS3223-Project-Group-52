package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

/**
 * The Scan class for the <i>distinct</i> operator.
 * 
 * @author Edward Sciore
 */
public class DistinctScan implements Scan {
	private SortScan s1 = null;
	private RecordComparator comp;
	private boolean hasmore1 = false;
	private HashMap<String, Constant> prev = new HashMap<>();
	private List<String> fields = new ArrayList<>();

	/**
	 * Create a distinct scan for the underlying scan.
	 * 
	 * @param s the underlying scan, which will be typecast as SortScan to sort it
	 * @param comp the record comparator to compare tuples for sorting
	 * @param fields the list of fields to compare across for distinct values
	 */
	public DistinctScan(Scan s, RecordComparator comp, List<String> fields) {
		this.s1 = (SortScan) s; // sorts the current scan
		this.comp = comp;
		this.fields = fields;
		s1.beforeFirst();
	}

	/**
	 * Position the scan before the first record in s1.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
		hasmore1 = s1.next();
	}

	/**
	 * Saves the previous tuple as a (field, value) pair in the prev hashmap
	 * The previous tuple is used to remove duplicates further in the scan.
	 * The previous tuple is updated in this function whenever a distinct value is seen.
	 * 
	 */
	public void updatePrev() {
		for (String field : this.fields) {
			Constant tempval = this.s1.getValue(field);
			prev.put(field, tempval);
		}
	}

	/**
	 * Compares the previous tuple and current tuple to check if they are distinct
	 * If they are distinct, the next() function will return true and update the previous tuple
	 * Otherwise if they are duplicates, the next() function will move to the next tuple without returning true
	 * 
	 * @param prev the value of the previous tuple
	 * @param s1 the current ongoing underlying scan
	 * @return if the previous value and current value are distinct from each other
	 */
	public boolean isDistinct(HashMap<String, Constant> prev, SortScan s1) {
		boolean hasdiff = false;
		for (String field : this.fields) { // compare value of every field
			Constant tempval = s1.getValue(field);
			if (prev.get(field).compareTo(tempval) != 0) { // if any value differs, they are distinct (not exact match)
				hasdiff = true;
			}
		}
		return hasdiff;
	}

	/**
	 * Performs the duplicate removal operation on a sorted scan of the current plan.
	 * The scan will compare the value of the current tuple with the value of the previous tuple across all fields.
	 * This comparison is done using the isDistinct() function.
	 * If they are distinct, the function will return true and update the previous tuple using updatePrev().
	 * Otherwise if they are duplicates, the function will move to the next tuple without returning true
	 * Once there are no more tuples in the scan, return false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		if (prev.isEmpty()) {
			updatePrev();

			hasmore1 = s1.next();

			return true;
		} 

		if (!hasmore1) { // end of scan
			return false;
		}
		while (hasmore1 = s1.next()) {
			if (isDistinct(this.prev, this.s1)) { // is distinct
				updatePrev();
				return true;
			} 
		}
		return false;
	}


	/**
	 * Return the integer value of the specified field. 
	 * 
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 * @return returns the integer value of the specified field
	 */
	public int getInt(String fldname) {
		return s1.getInt(fldname);
	}

	/**
	 * Return the string value of the specified field. 
	 * 
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 * @return returns the string value of the specified field
	 */
	public String getString(String fldname) {
		return s1.getString(fldname);
	}

	/**
	 * Return the value of the specified field. 
	 * 
	 * @param fldname the field name
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 * @return returns the value of the specified field
	 */
	public Constant getVal(String fldname) {
		return s1.getVal(fldname);
	}

	/**
	 * Return true if the specified field is in the underlying scans.
	 * 
	 * @param fldname the field name
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 * @return returns if the scan contains the fieldname
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname);
	}

	/**
	 * Close the scan by closing the underlying scans.
	 * 
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		s1.close();
	}

}
