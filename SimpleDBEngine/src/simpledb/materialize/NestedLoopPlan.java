package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>nestedloopjoin</i> operator.
 * @author Edward Sciore
 */
public class NestedLoopPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2, opr;
   private Schema sch = new Schema();
   
   /**
    * Creates a nestedloopjoin plan for the two specified queries.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
    */
   public NestedLoopPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, String opr) {
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.p1 = p1;
      this.p2 = p2;
      this.opr = opr;

      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   /** The method first opens a scan on each plan.
     * It then returns a nestedloopjoin scan
     * of the two table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new NestedLoopScan(s1, s2, fldname1, fldname2, opr);
      
   }
   
   /**
    * Return the number of block accesses required to
    * nestedloopjoin the tables.
    * The join is computed based on the formula |R|+|S|*||R||
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() + p2.blocksAccessed() * p1.recordsOutput();
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

