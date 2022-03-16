package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>distinct</i> operator.
 * 
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
	private Transaction tx;
	private Plan p;
	private Schema sch;
	private RecordComparator comp;
	private List<String> fields;

	/**
	 * Create a distinct plan for the underlying query. 
	 * The distinct operator will cause all fields to become distinct by our interpretation.
	 * Only distinct tuples will be returned by this plan/scan.
	 * 
	 * @param p           a plan for the underlying query
	 * @param tx 		  the underlying transaction
	 * @param fields      the list of fields to be selected distinctly
	 */
	public DistinctPlan(Transaction tx, Plan p, List<String> fields) {
		this.tx = tx;
		this.p = p;
		this.sch = p.schema();
		this.comp = new RecordComparator(fields);
		this.fields = fields;
	}

	/**
	 * This method opens a sort plan for the specified plan. The sort plan ensures
	 * that the underlying records will only contain distinct values.
	 * 
	 * @see simpledb.plan.Plan#open()
	 * @return returns the DistinctScan on the current plan, sorts it and removes duplicates
	 */
	public Scan open() {
		Scan s = p.open();
		return new DistinctScan(s, comp, fields);
	}

	/**
	 * Return the number of blocks required to compute the distinct values, which is one
	 * pass through the sorted table. It does <i>not</i> include the one-time cost
	 * of materializing and sorting the records.
	 * 
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Return the number of groups. Assuming equal distribution, this is the product
	 * of the distinct values for each grouping field.
	 * 
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p.recordsOutput();
	}

	/**
	 * Return the number of distinct values for the specified field. 
	 * The number of distinct values is the same as in the
	 * underlying query/field.
	 * 
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		if (p.schema().hasField(fldname))
			return p.distinctValues(fldname);
		else
			return recordsOutput();
	}

	/**
	 * Returns the schema of the output table. The schema consists of the selected
	 * fields.
	 * 
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return sch;
	}
	
}
