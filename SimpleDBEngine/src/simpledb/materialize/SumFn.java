package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class SumFn implements AggregationFn {
	   private String fldname;
	   private int sum;
	   
	   /**
	    * Create a sum aggregation function for the specified field.
	    * @param fldname the name of the aggregated field
	    */
	   public SumFn(String fldname) {
	      this.fldname = fldname;
	      sum = 0;
	   }
	   
	   /**
	    * Start a new sum to be the field value in the current record.
	    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
	    */
	   public void processFirst(Scan s) {
	      sum = s.getInt(fldname);
	   }
	   
	   /**
	    * Increase sum value with the current record by adding its int value to the sum.
	    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
	    */
	   public void processNext(Scan s) {
	      sum += s.getInt(fldname);
	   }
	   
	   /**
	    * Return the field's name, prepended by "sumof".
	    * @see simpledb.materialize.AggregationFn#fieldName()
	    */
	   public String fieldName() {
	      return "sumof" + fldname;
	   }
	   
	   /**
	    * Return the current sum.
	    * @see simpledb.materialize.AggregationFn#value()
	    */
	   public Constant value() {
	      return new Constant(sum);
	   }
}