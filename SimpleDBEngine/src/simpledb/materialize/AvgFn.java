package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn {
	   private String fldname;
	   private int count;
	   private int sum;
	   
	   /**
	    * Create a avg aggregation function for the specified field.
	    * @param fldname the name of the aggregated field
	    */
	   public AvgFn(String fldname) {
	      this.fldname = fldname;
	   }
	   
	   /**
	    * Start a new count and allocate sum to first value.
	    * Since SimpleDB does not support null values,
	    * every record will be counted,
	    * regardless of the field.
	    * The current count is thus set to 1 and sum is first value.
	    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
	    */
	   public void processFirst(Scan s) {
	      count = 1;
	      sum = s.getInt(fldname);
	   }
	   
	   /**
	    * Since SimpleDB does not support null values,
	    * this method always increments the count, and adds to the sum,
	    * regardless of the field.
	    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
	    */
	   public void processNext(Scan s) {
	      count++;
	      sum += s.getInt(fldname);
	   }
	   
	   /**
	    * Return the field's name, prepended by "avg".
	    * @see simpledb.materialize.AggregationFn#fieldName()
	    */
	   public String fieldName() {
	      return "avgof" + fldname;
	   }
	   
	   /**
	    * Return the current avg using the floor value of sum divided by count.
	    * @see simpledb.materialize.AggregationFn#value()
	    */
	   public Constant value() {
	      return new Constant(sum/count);
	   }
	}

