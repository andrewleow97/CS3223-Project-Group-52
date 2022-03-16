package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * The Scan class for the <i>groupby</i> operator.
 * 
 * @author Edward Sciore
 */
public class AggregateScan implements Scan {
	private Scan s;
	private List<AggregationFn> aggfns;
	private boolean aggnext;

	/**
	 * Create a groupby scan, given a grouped table scan.
	 * 
	 * @param s           the grouped scan
	 * @param groupfields the group fields
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
	 * Move to the next group. The key of the group is determined by the group
	 * values at the current record. The method repeatedly reads underlying records
	 * until it encounters a record having a different key. The aggregation
	 * functions are called for each record in the group. The values of the grouping
	 * fields for the group are saved.
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
	 * Get the Constant value of the specified field. If the field is a group field,
	 * then its value can be obtained from the saved group value. Otherwise, the
	 * value is obtained from the appropriate aggregation function.
	 * 
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
				return fn.value();
		throw new RuntimeException("field " + fldname + " not found.");
	}

	/**
	 * Get the integer value of the specified field. If the field is a group field,
	 * then its value can be obtained from the saved group value. Otherwise, the
	 * value is obtained from the appropriate aggregation function.
	 * 
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public int getInt(String fldname) {
		return getVal(fldname).asInt();
	}

	/**
	 * Get the string value of the specified field. If the field is a group field,
	 * then its value can be obtained from the saved group value. Otherwise, the
	 * value is obtained from the appropriate aggregation function.
	 * 
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public String getString(String fldname) {
		return getVal(fldname).asString();
	}

	/**
	 * Return true if the specified field is either a grouping field or created by
	 * an aggregation function.
	 * 
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
				return true;
		return false;
	}
}
