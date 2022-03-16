package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * The Scan class for the <i>aggregate</i> operator.
 * 
 * @author Edward Sciore
 */
public class AggregateScan implements Scan {
	private Scan s;
	private List<AggregationFn> aggfns;
	private boolean aggnext;

	/**
	 * Create an aggregate scan, given a aggregate table scan.
	 * 
	 * @param s           the grouped scan
	 * @param aggfns      the aggregation functions
	 */
	public AggregateScan(Scan s, List<AggregationFn> aggfns) {
		this.s = s;
		this.aggfns = aggfns;
		beforeFirst();
	}

	/**
	 * Position the scan before the first group. Internally, the underlying scan is
	 * always positioned at the first record of a group, which means that this
	 * method moves to the first underlying record.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s.beforeFirst();
		aggnext = s.next();
	}

	/**
	 * Move to the next aggregation function on a grouped scan.
	 * For each aggregation function, process the first tuple, and process each of the next aggregation functions
	 * while there are tuples remaining in the scan.
	 * Returns false when there are no more tuples.
	 * 
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		if (!aggnext)
			return false;
		for (AggregationFn fn : aggfns) {
			fn.processFirst(s);
		}
		while (aggnext = s.next()) {
			for (AggregationFn fn : aggfns)
				fn.processNext(s);
		}
		
		return true;

	}

	/**
	 * Close the scan by closing the underlying scan.
	 * 
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		s.close();
	}

	/**
	 * Get the Constant value of the specified field. 
	 * The value is obtained from the appropriate aggregation function value().
	 * If the aggregation function does not contain the fieldname, throw a runtime exception.
	 * 
	 * @param fldname the field name to get the value from
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
				return fn.value();
		throw new RuntimeException("field " + fldname + " not found.");
	}

	/**
 	 * Get the integer value of the specified field. 
	 * The value is obtained from the appropriate aggregation function getVal() typecast as integer.
	 * 
	 * @param fldname the field name to get the value from
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public int getInt(String fldname) {
		return getVal(fldname).asInt();
	}

	/**
 	 * Get the integer value of the specified field. 
	 * The value is obtained from the appropriate aggregation function getVal() typecast as string.
	 * 
	 * @param fldname the field name to get the value from
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public String getString(String fldname) {
		return getVal(fldname).asString();
	}

	/**
	 * Return true if the specified field exists within the list of aggregation functions.
	 * 
	 * @param fldname the field name to check
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
				return true;
		return false;
	}
}
