package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class HashJoinPlan implements Plan {
   private HashPartitionPlan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private Transaction tx;
   
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
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
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
      HashMap<Integer, TempTable> partition1 = p1.partition(); 

      HashMap<Integer, TempTable> partition2 = p2.partition(); 
      // 2 hashmap of partitions
      // join into 1 hashmap of partitions -> scan on this one
      
      //s1 hash table for comparison for final output
      // rehash s1
      // for temptable in s2
      	// rehash s2 append into s1
      	// output s1 temptable if s2 added in else delete
     // 
      
      //record comparator compare function (returns 0 if matching)
      
      HashMap<Integer, TempTable> output = new HashJoinScan(tx, partition1, partition2, fldname1, fldname2).next();
      
      
      
      List<Integer> outputKeys = new ArrayList<>();
		// rehash s1 into output
		for (int key : s1.keySet()) { // key = s1/s2 partition k
			Schema schema = s1.get(key).getLayout().schema();
			Scan s = s1.get(key).open();
			s.beforeFirst();
			int hash = 0;
			while (s.next()) { // scan of temptable of partition k in s1
				try {
					int joinval = s.getInt(fldname1);
					hash = joinval % hashval;

				} catch (NumberFormatException e) { // not an int
					String joinval = s.getString(fldname1);
					hash = ((joinval == null) ? 0 : joinval.hashCode()) % hashval;

				}
				UpdateScan insert = output.get(hash).open();
				insert.insert();
				for (String fldname : schema.fields()) {
					insert.setVal(fldname, s.getVal(fldname));
				}
			}
			// 1 partition rehashed
			// check same key in s2 for matching
			schema = s2.get(key).getLayout().schema();
			Scan scan2 = s2.get(key).open();
			scan2.beforeFirst();
			hash = 0;
			while (scan2.next()) { // scan of temptable of partition k in s2
				try {
					int joinval = s.getInt(fldname1);
					hash = joinval % hashval;
				} catch (NumberFormatException e) { // not an int
					String joinval = s.getString(fldname1);
					hash = ((joinval == null) ? 0 : joinval.hashCode()) % hashval;
				}
		}
		// s1 3 -> 4, s2 10 -> 4
		// maintain list of all keys in output in list here
		for (int key : s2.keySet()) {
			Schema schema = s2.get(key).getLayout().schema();
			Scan s = s2.get(key).open();
			s.beforeFirst();
			int hash = 0;
			while (s.next()) {
				try {
					int joinval = s.getInt(fldname1);
					hash = joinval % hashval;
				} catch (NumberFormatException e) { // not an int
					String joinval = s.getString(fldname1);
					hash = ((joinval == null) ? 0 : joinval.hashCode()) % hashval;
				}
				if (output.containsKey(hash)) {
					// insert s2 val
					UpdateScan insert = output.get(hash).open();
					insert.insert();
					for (String fldname : schema.fields()) {
						insert.setVal(fldname, s.getVal(fldname));
					}
					if (!outputKeys.contains(hash)) {
						outputKeys.add(hash);
					}
				}
			}
		}
		// output = all s1 and only matching s2 tuples
		for (int key : output.keySet()) {
			if (!outputKeys.contains(key)) {
				output.remove(key);
			}
		}
		return output; // only matching s1 & s2 temptables according to key
      //returning join
   }
   
   /**
    * Return the number of block acceses required to
    * mergejoin the sorted tables.
    * Since a mergejoin can be preformed with a single
    * pass through each table, the method returns
    * the sum of the block accesses of the 
    * materialized sorted tables.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() { // partition = 2(M+N), matching = (M+N)
      return 3 * (p1.blocksAccessed() + p2.blocksAccessed());
   }
   
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }
   
   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}



