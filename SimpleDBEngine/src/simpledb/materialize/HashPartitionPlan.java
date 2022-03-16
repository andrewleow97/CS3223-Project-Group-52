package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the partition phase of the <i>hashjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashPartitionPlan implements Plan {
	private Plan p;
	private String fldname1;
	private Schema sch = new Schema();
	private Transaction tx;
	private int k;

	/**
	 * Creates a hashpartition plan for the two specified queries. 
	 * The plan will be partitioned into a hash table with buckets according to the amount of available buffers.
	 * 
	 * @param p        the query plan
	 * @param fldname1 the join field
	 * @param tx       the calling transaction
	 */
	public HashPartitionPlan(Transaction tx, Plan p, String fldname1) {
		this.fldname1 = fldname1;
		this.p = p;
		this.tx = tx;
		sch.addAll(p.schema());
		this.k = this.tx.availableBuffs() - 1;
	}

	/**
	 * The method opens a Scan on the query plan, and splits the scan into multiple 
	 * buckets of TempTables based on the hash of the value in the joinfield.
	 * 
	 * 
	 * @return output the partitioned scan of Plan p
	 */
	public HashMap<Integer, TempTable> partition() {
		Scan s = p.open();
		HashMap<Integer, TempTable> output = splitIntoRuns(s);
		return output;
	}

	/**
	 * Open a scan on the current plan.
	 * 
	 * @see simpledb.plan.Plan#open()
	 * @return the Scan of the current plan
	 */
	public Scan open() {
		return p.open();
	};

	/**
	 * Return the number of block acceses required to hashjoin the sorted tables.
	 * Since a hashjoin partitioning can be done with a single pass through each table, the
	 * method returns the blocks accessed by the current plan = |R|.
	 * 
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() { // partition = 2(M+N), matching = (M+N)
		return p.blocksAccessed();
	}

	/**
	 * Return the number of records in the partition.
	 * 
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p.recordsOutput();
	}

	/**
	 * Estimate the distinct number of field values in the join. Since the join does
	 * not increase or decrease field values, the estimate is the same as in the
	 * appropriate underlying query.
	 * 
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname1) {
		if (p.schema().hasField(fldname1))
			return p.distinctValues(fldname1);
		return 0;
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

	/**
	 * Calculates the hash value of integer x based on the number of available buffers.
	 * Determines which bucket the tuple will be hashed into.
	 * 
	 * @param x the integer value of the join field
	 * @return the hash value of x
	 */
	public int hashInt(int x) {
		return x % k;
	}

	/**
	 * Calculates the hash value of String x based on the number of available buffers.
	 * Done by using the hashCode of String x.
	 * Determines which bucket the tuple will be hashed into.
	 * 
	 * @param x the String value of the join field
	 * @return the hash value of x
	 */
	public int hashString(String x) {
		int result = ((x == null) ? 0 : x.hashCode());
		return result % k;
	}

	/**
	 * Partitions the scan into a few TempTables as per Grace Hash Join partitioning phase
	 * The number of buckets is determined by B, the number of available buffers - 1.
	 * The appropriate bucket is chosen using the hashInt() or hashString() functions.
	 * The values are copied from the scan into the TempTable for all the schema fields.
	 * 
	 * @param src the scan on the query plan
	 * @return the partitioned table of the scan
	 */
	private HashMap<Integer, TempTable> splitIntoRuns(Scan src) {
		HashMap<Integer, TempTable> temps = new HashMap<>();
		// intialise the hashmap with empty temptables
		for (int i = 0; i < k; i++) {
			TempTable currenttemp = new TempTable(tx, sch);
			temps.put(i, currenttemp);
		}
		if (!src.next())
			return temps;
		src.beforeFirst();

		while (src.next()) {
			int hash = 0;
			// choose the correct bucket
			try {
				int joinval = src.getInt(fldname1);
				hash = hashInt(joinval);

			} catch (NumberFormatException e) { // not an int
				String joinval = src.getString(fldname1);
				hash = hashString(joinval);

			}
			// insert into the correct partition
			UpdateScan currscan = temps.get(hash).open();
			currscan.insert();
			for (String fldname : sch.fields()) {
				currscan.setVal(fldname, src.getVal(fldname));
			}
			currscan.close();

		}
		src.close();
		return temps;
	}
	
}
