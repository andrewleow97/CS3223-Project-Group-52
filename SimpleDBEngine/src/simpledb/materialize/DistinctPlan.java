package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>groupby</i> operator.
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
	 * Create a groupby plan for the underlying query. The grouping is determined by
	 * the specified collection of group fields, and the aggregation is computed by
	 * the specified collection of aggregation functions.
	 * 
	 * @param p           a plan for the underlying query
	 * @param groupfields the group fields
	 * @param aggfns      the aggregation functions
	 * @param tx          the calling transaction
	 */
	public DistinctPlan(Transaction tx, Plan p, List<String> fields) { // [sname, majorid, gradyear]
		this.tx = tx;
		this.p = p;
		this.sch = p.schema();
		this.comp = new RecordComparator(fields);
		this.fields = fields;
	}

	/**
	 * This method opens a sort plan for the specified plan. The sort plan ensures
	 * that the underlying records will be appropriately grouped.
	 * 
	 * @see simpledb.plan.Plan#open()
	 */
	public Scan open() {
		Scan s = p.open();
		return new DistinctScan(s, comp, fields);
	}

	/**
	 * Return the number of blocks required to compute the aggregation, which is one
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
	 * Return the number of distinct values for the specified field. If the field is
	 * a grouping field, then the number of distinct values is the same as in the
	 * underlying query. If the field is an aggregate field, then we assume that all
	 * values are distinct.
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
	 * Returns the schema of the output table. The schema consists of the group
	 * fields, plus one field for each aggregation function.
	 * 
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return sch;
	}
}
