package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>aggregate</i> operator.
 * @author Edward Sciore
 */
public class AggregatePlan implements Plan {
   private Plan p;
   private List<AggregationFn> aggfns;
   private Schema sch = new Schema();
   
   /**
    * Create a aggregate plan for the underlying query.
    * The aggregation is determined by the specified
    * collection of aggregation functions.
    * 
    * @param p a plan for the underlying query
    * @param aggfns the aggregation functions
    * @param tx the calling transaction
    */
   public AggregatePlan(Transaction tx, Plan p,List<AggregationFn> aggfns) { 
      this.aggfns = aggfns;
      this.p = p;
      for (AggregationFn fn : aggfns) {
         sch.addIntField(fn.fieldName());
      }
   }
   
   /**
    * This method opens a scan for the specified aggregation plan.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new AggregateScan(s, aggfns);
   }
   
   /**
    * Return the number of blocks required to
    * compute the aggregation,
    * which is one pass through the sorted table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Return the number of groups.  
    * Assuming single aggregation values for each group, this value is 1.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int numgroups = 1;
      return numgroups;
   }
   
   /**
    * Return the number of distinct values for the specified field.  
    * We assume that all values are distinct.
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
    * Returns the schema of the output table.
    * The schema consists of all the selection fields and aggregation functions.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
}
