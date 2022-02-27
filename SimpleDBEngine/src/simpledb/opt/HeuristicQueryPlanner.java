package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;
import simpledb.materialize.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan(); 
      
      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }
     
      // Step 4.  Project on the field names and return
      currentplan = new ProjectPlan(currentplan, data.fields());
      
      // If no sorting is specified
      if (data.sortFields() == null || data.sortFields().isEmpty()) {
    	  return currentplan;
      }
      
//      // Aggregate
//      if (data.aggFns() == null || data.aggFields() == null) {
//    	  return currentplan;
//      } else {
//    	  
//      }
      
      // Step 5. Group by
      if (data.groupList() != null && data.aggFields() != null) {
    	  currentplan = new GroupByPlan(tx, currentplan, data.groupList(), data.aggFields());
      } 
      
      // Step 6. Sort the final plan node   
      return new SortPlan(tx, currentplan, data.sortFields(), data.sortOrder());
       
      
      
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan indexPlan = tp.makeIndexJoinPlan(current);       
         Plan sortMergePlan = tp.makeSortMergePlan(current);        
         Plan nestedLoopPlan = tp.makeNestedLoopPlan(current);
         Plan hashJoinPlan = tp.makeHashJoinPlan(current);
//         bestplan = compare(indexPlan, sortMergePlan, nestedLoopPlan);
         bestplan = hashJoinPlan;
//         System.out.printf("%s %d\n", "index", indexPlan.blocksAccessed());
//         System.out.printf("%s %d\n", "sortmerge", sortMergePlan.blocksAccessed());
//         System.out.printf("%s %d\n", "nestedloop", nestedLoopPlan.blocksAccessed());
//         System.out.printf("%s %d\n", "hashjoin", hashJoinPlan.blocksAccessed());
//         
//         System.out.printf("%s %d\n", "index", indexPlan.recordsOutput());
//         System.out.printf("%s %d\n", "sortmerge", sortMergePlan.recordsOutput());
//         System.out.printf("%s %d\n", "nestedloop", nestedLoopPlan.recordsOutput());
//         System.out.printf("%s %d\n", "hashjoin", hashJoinPlan.recordsOutput());
         if (bestplan != null)
        	 besttp = tp;
//         if (indexPlan != null && (bestplan == null || indexPlan.recordsOutput() < bestplan.recordsOutput())) {
//             besttp = tp;
//             bestplan = indexPlan;
//          }
      }
      if (bestplan != null)
         tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan compare(Plan index, Plan sortmerge, Plan nested) {
	   int indexblocks = index.blocksAccessed() + index.recordsOutput();
	   int sortblocks = sortmerge.blocksAccessed() + sortmerge.recordsOutput();
	   int nestedblocks = nested.blocksAccessed() + nested.recordsOutput();
	   if (indexblocks <= sortblocks && indexblocks <= nestedblocks) {
		   System.out.println("index chosen");
		   return index;
	   } else if (sortblocks <= indexblocks && sortblocks <= nestedblocks) {
		   System.out.println("sortmerge chosen");
		   return sortmerge;
	   } else {
		   System.out.println("nested chosen");
		   return nested;
	   }
   }
   

   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
