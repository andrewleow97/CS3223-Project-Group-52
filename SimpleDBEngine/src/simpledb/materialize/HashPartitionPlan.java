package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashPartitionPlan implements Plan {
	private Plan p;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	private Transaction tx;
	private int k;

	/**
	 * Creates a mergejoin plan for the two specified queries. The RHS must be
	 * materialized after it is sorted, in order to deal with possible duplicates.
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
	 * The method first sorts its two underlying scans on their join field. It then
	 * returns a mergejoin scan of the two sorted table scans.
	 * 
	 * @see simpledb.plan.Plan#open()
	 */
	public HashMap<Integer, TempTable> partition() {
		Scan s = p.open();
		HashMap<Integer, TempTable> output = splitIntoRuns(s);
		return output;
	}

	public Scan open() {
		return p.open();
	};

	/**
	 * Return the number of block acceses required to mergejoin the sorted tables.
	 * Since a mergejoin can be preformed with a single pass through each table, the
	 * method returns the sum of the block accesses of the materialized sorted
	 * tables. It does <i>not</i> include the one-time cost of materializing and
	 * sorting the records.
	 * 
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() { // partition = 2(M+N), matching = (M+N)
		return p.blocksAccessed();
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
		return p.distinctValues(fldname1);
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

	public int hashInt(int x) {
		return x % k;
	}

	public int hashString(String x) {
		int result = ((x == null) ? 0 : x.hashCode());
		return result % k;
	}

//   
	private HashMap<Integer, TempTable> splitIntoRuns(Scan src) {
		HashMap<Integer, TempTable> temps = new HashMap<>();
		for (int i = 0; i < k; i++) {
			TempTable currenttemp = new TempTable(tx, sch);
			temps.put(i, currenttemp);
		}
		if (!src.next())
			return temps;
		src.beforeFirst();

		while (src.next()) {
			int hash = 0;
			try {
				int joinval = src.getInt(fldname1);
				hash = hashInt(joinval);

			} catch (NumberFormatException e) { // not an int
				String joinval = src.getString(fldname1);
				hash = hashString(joinval);

			}
			UpdateScan currscan = temps.get(hash).open();
			currscan.insert();
			for (String fldname : sch.fields()) {
				currscan.setVal(fldname, src.getVal(fldname));
			}
			// currentscan is the whole record -> add to temptable in position hash
			currscan.close();

		}
		src.close();
		return temps;
	}
	
}
