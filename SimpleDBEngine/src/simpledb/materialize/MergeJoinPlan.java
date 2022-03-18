package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

import java.lang.Math;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class MergeJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private Transaction tx;
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param tx the calling transaction
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
   public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
	  this.tx = tx;
      this.fldname1 = fldname1;
      List<String> sortlist1 = Arrays.asList(fldname1);
      this.p1 = new SortPlan(tx, p1, sortlist1);

      this.fldname2 = fldname2;
      List<String> sortlist2 = Arrays.asList(fldname2);
      this.p2 = new SortPlan(tx, p2, sortlist2);
  
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
      Scan s1 = p1.open();
      SortScan s2 = (SortScan) p2.open();
      return new MergeJoinScan(s1, s2, fldname1, fldname2);
   }
   
   /**
    * Return the number of block accesses required to
    * mergejoin the sorted tables.
    * Since a mergejoin can be preformed with a single
    * pass through each table, the method returns
    * the sum of the block accesses of the 
    * materialized sorted tables.
    * Computes the cost of sorting the two tables and merging.
    * Assumes cost to sort and merge follows formula taught in lecture.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
	   double R_B = Math.ceil((double) p1.blocksAccessed()/tx.availableBuffs());
	   double top1 = Math.log(R_B);
	   double bottom1 = Math.log(tx.availableBuffs()-1);
	   int val1 = (int) Math.ceil(top1/bottom1);
	   int p1cost =  2 * p1.blocksAccessed() * (1+val1);
	   
	   double S_B = Math.ceil((double) p2.blocksAccessed()/tx.availableBuffs());
	   double top2 = Math.log(S_B);
	   double bottom2 = Math.log(tx.availableBuffs()-1);
	   int val2 = (int) Math.ceil(top2/bottom2);
	   int p2cost =  2 * p2.blocksAccessed() * (1+val2);
      return (p1cost + p2cost) + (p1.blocksAccessed() + p2.blocksAccessed()); // cost of sort phase + cost of merge
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

