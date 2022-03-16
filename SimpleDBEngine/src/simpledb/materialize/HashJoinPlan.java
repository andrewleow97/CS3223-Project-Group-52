package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for merging phase of the <i>hashjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinPlan implements Plan {
	private HashPartitionPlan p1, p2;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	private Transaction tx;

	/**
	 * Creates a hashjoin plan for the two specified queries.
	 * 
	 * @param p1       the LHS partitioned query plan
	 * @param p2       the RHS partitioned query plan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param tx       the calling transaction
	 */
	public HashJoinPlan(Transaction tx, HashPartitionPlan p1, HashPartitionPlan p2, String fldname1, String fldname2) {
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.p1 = p1;
		this.p2 = p2;
		this.tx = tx;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
	}

	/**
	 * The method first partitions its two plans into hash partitions. It then
	 * returns a hashjoin scan of the two partitions.
	 * 
	 * @see simpledb.plan.Plan#open()
	 * @return out the hashjoin scan of the two partitions
	 */
	public Scan open() {
	

		// partition each scan first
		HashMap<Integer, TempTable> partition1 = p1.partition();
		HashMap<Integer, TempTable> partition2 = p2.partition();
	
		// perform merging of both partitions
		HashJoinScan out = new HashJoinScan(tx, partition1,partition2,fldname1,fldname2,sch);
	
		return out;
	}
	

	/**
	 * Return the number of block acceses required to hashjoin the two tables.
	 * Since a hashjoin merge can be preformed with a single pass through each table, the
	 * method returns the sum of the block accesses of the partitioned tables.
	 * 
	 * @see simpledb.plan.Plan#blocksAccessed()
	 * @return return the total cost of the grace hash join
	 */
	public int blocksAccessed() { // partition = 2(M+N), matching = (M+N)
		return 3 * (p1.blocksAccessed() + p2.blocksAccessed());
	}

	/**
	 * Return the number of records in the join. Assuming uniform distribution, the
	 * formula is:
	 * 
	 * <pre>
	 *  R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
	 * </pre>
	 * 
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	}

	/**
	 * Estimate the distinct number of field values in the join. Since the join does
	 * not increase or decrease field values, the estimate is the same as in the
	 * appropriate underlying query.
	 * 
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	/**
	 * Return the schema of the join, which is the union of the schemas of the
	 * underlying queries.
	 * 
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return sch;
	}
	
}
