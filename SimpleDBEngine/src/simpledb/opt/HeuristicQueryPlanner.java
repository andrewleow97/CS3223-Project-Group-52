package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;
import simpledb.materialize.*;
import simpledb.query.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * 
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
	private Collection<TablePlanner> tableplanners = new ArrayList<>();
	private MetadataMgr mdm;
	private HashMap<String, ArrayList<String>> queryPlan = new HashMap<>();
	public HeuristicQueryPlanner(MetadataMgr mdm) {
		this.mdm = mdm;
	}

	/**
	 * Creates an optimized left-deep query plan using the following heuristics. H1.
	 * Choose the smallest table (considering selection predicates) to be first in
	 * the join order. H2. Add the table to the join order which results in the
	 * smallest output.
	 *
	 */
	public Plan createPlan(QueryData data, Transaction tx) {
		
		queryPlan = new HashMap<>();
		queryPlan.computeIfAbsent("spred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("jpred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("join", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>());
		
		//Adding all fields to the query plan
		for(String d : data.aggOrder()) {
		      queryPlan.computeIfAbsent("field", k -> new ArrayList<>()).add(d);
		    }
		
		//Adding join and selection predicate into query plan
		for (Term term : data.pred().terms) {
			if (term.compareField()) {
				// Join predicate a = b
				queryPlan.computeIfAbsent("jpred", k -> new ArrayList<>()).add(term.toString());
			} else {
				// a > 5
				queryPlan.computeIfAbsent("spred", k -> new ArrayList<>()).add(term.toString());
			}
		}
		
		// Step 1: Create a TablePlanner object for each mentioned table
		for (String tblname : data.tables()) {
			TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
			tableplanners.add(tp);
		}

		// Step 2: Choose the lowest-size plan to begin the join order
		Plan currentplan = getLowestSelectPlan();		
		
		// Step 3: Repeatedly add a plan to the join order
		while (!tableplanners.isEmpty()) {
			Plan p = getLowestJoinPlan(currentplan);
			if (p != null)
				currentplan = p;
			else // no applicable join
				currentplan = getLowestProductPlan(currentplan);
		}
		
		// Step 4. Project on the field names and return
		currentplan = new ProjectPlan(currentplan, data.fields());

		// Step 5
		
		//Aggregate functions are being used and no group by clause
		if (data.aggFields() != null && data.groupList() == null && data.aggFields().size() > 0) {
			currentplan = new AggregatePlan(tx, currentplan, data.aggFields());
		}

		// group by clause present or aggregate functions used
		else if (data.groupList() != null || data.aggFields() != null) {
			currentplan = new GroupByPlan(tx, currentplan, data.fields(), data.groupList(), data.aggFields(), data.aggOrder());
		}
		
		//System.out.println(queryPlan);
		getQueryPlan(); // Output query plan
		
		
		// Step 6
		
		/**
		 * If ordering is not required and if distinct is required, sort by 
		 * the selection fields and eliminate duplicates 
		 * else (distinct not required) return current plan.
		 * 
		 * Else (ordering is required) and if distinct is required,
		 * add select fields (that are not in sort fields) to sort fields and add default "asc" to sort order.
		 * Then sort by sort fields and sort order and eliminate duplicate.
		 * else(distinct not required) just sort by sort fields and sort order. 
		 */
		
		//If ordering is not required
		if (data.sortFields() == null || data.sortFields().isEmpty()) {
			//If require distinct records
			if (data.isDistinct() != false) {
				currentplan = new SortPlan(tx, currentplan, data.fields());	//sort plan
				currentplan = new DistinctPlan(tx, currentplan, data.fields());	//eliminate duplicates
			}
			return currentplan;
		} 
		else {  //If ordering is required.
			//If require distinct records
			if (data.isDistinct() != false) {
				
				//Add select fields that are not in sort fields to sort fields
				for (String field : data.fields()) {
					if (!data.sortFields().contains(field)) { 
						data.sortFields().add(field);
						data.sortOrder().add("asc");
					}
				}
				currentplan = new SortPlan(tx, currentplan, data.sortFields(), data.sortOrder()); //sort plan
				return new DistinctPlan(tx, currentplan, data.fields()); //eliminate duplicates
			} else {
				//just return, no duplicate elimination
				currentplan = new SortPlan(tx, currentplan, data.sortFields(), data.sortOrder()); //sortplan
				return currentplan;
			}
		}

	}

	
	/**
	 * Construct the query plan and prints out the query plan.
	 * 
	 * Format:
	 * select (“project fields”)(“select predicate”)[(“SelectionScan”) “type of join” (“SelectionScan”)] (“join predicate”)
	 * Example:
	 * select (sid)(dname) [(dept) IndexBasedJoin with student (majorid(hash) on student) SortMergeJoin with course (course) NestedLoopsJoin with enroll (enroll)](majorid=did)(did=deptid)(sid=studentid)
	 * 
	 * field - all the fields to be projected
	 * spred - select predicate
	 * jpred - join predicate
	 * table - tables in the query
	 * join - join algorithm used
	 * joinindex - index used during index nested loop join (Format [table name, field name(index type)])
	 * selectindex - index used when we used the select pred
	 * 
	 * Note: index of table and join are align. We can use the index of table to iterate through the join list
	 * but take note that size of join is always 1 less than size of table.
	 * Note: sequence of indexBasejoin in join corresponds to the sequence of joinindex.
	 * 
	 */
	private void getQueryPlan() {
		int iJoin = 0; //Keeps track of the "join" algorithm index.
		String s = "";
		s += "select ";
		
		//Add all the project fields
		for(String d: queryPlan.get("field")) {
			s += "(" + d + ")";
		}
		
		// Add all the select pred
		if (queryPlan.get("spred").size() != 0) {
			for(String sp : queryPlan.get("spred")) {
				s += "(" + sp + ")";
			}
		}
		
		// If there is a join
		if (queryPlan.get("jpred").size() != 0) {
			s += " [";
			
			for(int i = 0; i < queryPlan.get("table").size(); i++) {
				String table = queryPlan.get("table").get(i);
				
				// since size of join is 1 less of table.
				if (iJoin > queryPlan.get("join").size()-1)
					iJoin--;
		        
				// If current join algorithm is an index nested loop join
				if (queryPlan.get("join").get(iJoin).contains("Index")) {
		          int indexAtJoinIndex = queryPlan.get("joinindex").indexOf(table); //Get index of table. indexAtJoinIndex is table name, indexAtJoinIndex + 1 is index type.
		          
		          // joinindex does not contain an index on current table, means is normal scan of table. Do not increment ijoinindex as table is not using any index.
		          if (indexAtJoinIndex < 0) {
		        	  if(i == 0 && queryPlan.get("selectindex").size() > 0) {
		        		  s += "(" + queryPlan.get("selectindex").get(i*2 + 1) + " select on " + queryPlan.get("table").get(i) + ")";
		        	  } else {
		        		  s += "(" + table + ")";
		        	  }
		            s += " " + queryPlan.get("join").get(i) + " "; //append join algo
		            continue;
		          }
		            
		          // joinindex contains current table, get indextype used
		           else {
			          String joinIndexType = queryPlan.get("joinindex").get(indexAtJoinIndex + 1); // type of index
			          s += "(" + joinIndexType + " on " + table + ")";
			          if (i != queryPlan.get("table").size()-1)
			        	  s += " " + queryPlan.get("join").get(i) + " ";       
			          iJoin++; // move on to the next join algo
			          continue;
		          }
		          
		        // table @ iJoinIndex is not an index join algo, means is normal scan of table.
		        } else {
		        	if(i == 0 && queryPlan.get("selectindex").size() > 0) {
		        		  s += "(" + queryPlan.get("selectindex").get(i*2 + 1) + " select on " + queryPlan.get("table").get(i) + ")";
		        	  } else {
		        		  s += "(" + table + ")";
		        	  }
		          if (i != queryPlan.get("table").size()-1) {
		            s += " " + queryPlan.get("join").get(i) + " ";
		          }
		          iJoin++; // move on to the next join algo
		        }
			}
			s += "] ";
			
			//add join predicate to string s
			for(String jp : queryPlan.get("jpred")) {
				s += "(" + jp + ")";
			}
		}
		//Else there is no join predicate
		else {
			s += "[";
			for(int i = 0; i < queryPlan.get("table").size(); i++) {
				// If there is select pred.
				if(!queryPlan.get("selectindex").isEmpty()) {
					String fldname;
					try {
						fldname = queryPlan.get("selectindex").get(i*2);
					} catch (IndexOutOfBoundsException e) {
						fldname = null;
					}
					//If there is an index used on the select pred
					if(fldname != null) {
						s += "(" + queryPlan.get("selectindex").get(i*2 + 1) + " select on " + queryPlan.get("table").get(i) + ")";
					} else {
						s += "(" + queryPlan.get("table").get(i) + ")";
					}
					try {
						s += queryPlan.get("join").get(i);
					} catch (IndexOutOfBoundsException e) {
						continue;
					}
				} else {
					s += "(" + queryPlan.get("table").get(i) + ")";
					try {
						s += queryPlan.get("join").get(i);
					} catch (IndexOutOfBoundsException e) {
						continue;
					}
				}
			}
			s += "]";
		}
		
		System.out.println(s);
	}
	
	
	/**
	 * This method gets the plan with the lowest record output 
	 * by iterating through all table planners and updating
	 * the bestplan if the current plan has less record output 
	 * then best plan's.
	 * 
	 * @return the plan with the lowest record output
	 */
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
		HashMap<String, String> indexUsed = new HashMap<>(); // Store the index used when executing the select pred by the lowest plan. key: table name and value: field(index type used)
		indexUsed = besttp.getIndexUsedSelectPlan();
		// Add to select index using the format [tablename1, fieldname1(index type)]
		for (String i : indexUsed.keySet()) {
			queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>()).add(i);
			queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>()).add(indexUsed.get(i));
		}

		// Add the lowestplan table name as the first table in table
		queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(besttp.myplan.tblname);
		
		tableplanners.remove(besttp);
		return bestplan;
	}

	/**
	 * This method keeps track of 2 plans, currentplan and bestplan.
	 * For each table planner, 5 plans are initialise, IndexJoinplan, SortMergePlan, 
	 * NestedLoopPlan, HashJoinPlan and ProductPlan. If it has a join predicate and
	 * the operator is "=", we compare all the plans using the compare method to get 
	 * the best current plan. Else if the an non equi join, we will default current plan
	 * to be nestedLoopPlan. Else there is no joinPred, we will default to ProductPlan.
	 * If the currentplan is less than the bestplan, we will update the bestplan 
	 * with the currentplan.
	 * Additionally, we will update the queryPlan, updating the join plan used, the 
	 * join predicate and table.
	 * 
	 * @param current the specified plan
	 * @return the best plan
	 */
	
	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		Plan currentplan = null;
		for (TablePlanner tp : tableplanners) {
			
			// Get plans from all algorithms.
			Plan indexPlan = tp.makeIndexJoinPlan(current);
			Plan sortMergePlan = tp.makeSortMergePlan(current);
			Plan nestedLoopPlan = tp.makeNestedLoopPlan(current);
			Plan hashJoinPlan = tp.makeHashJoinPlan(current);
			Plan productPlan = tp.makeDefaultProductPlan(current);
			
			Predicate joinPred = tp.returnJoinPredicate(current); // Get join pred.
			// If current joinPred is an equi join.
			if (joinPred != null && joinPred.terms.get(0).operator().equals("=")) {
				currentplan = compare(indexPlan, sortMergePlan, nestedLoopPlan, hashJoinPlan, tp);
			}	
			// If current joinPred is an non-equi join, we default the join to be nestedLoopPlan.
			else if (joinPred != null){
				currentplan = nestedLoopPlan;
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("NestedLoopsJoin with " + tp.myplan.tblname);
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname);
			}
			// If there is no joinPred, we default the join to be ProductPlanJoin.
			else {
				currentplan = tp.makeDefaultProductPlan(current);
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("ProductPlanJoin with " + tp.myplan.tblname);
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname);
			}

			
			if (currentplan != null) {
				if (bestplan == null) {
					bestplan = currentplan;
					besttp = tp;
				} else if ((bestplan.blocksAccessed() + bestplan.recordsOutput()) > (currentplan.blocksAccessed() + currentplan.recordsOutput())) { // If currentplan < bestplan
					bestplan = currentplan; // replace bestplan with currentplan
					besttp = tp; // replace tableplanner
					// update queryplan
					queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-2); // remove the prev join (2nd last) algo from prev tp
					queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-2); // remove the prev table (2nd last) from prev tp
					
				} else { // If bestplan <= currentplan
					queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-1); // remove the last join algo from current tp
					queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-1); // remove the last table from current tp
				}
				
			} else {
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-1); // remove the last join algo from current tp
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-1); // remove the last table from current tp
			}
		}
		if (bestplan != null) {
			// update queryPlan so that the 
			// If prev join algorithm is an index-nestedloop join
			if (queryPlan.containsKey("join") && queryPlan.get("join").get(queryPlan.get("join").size()-1).contains("Index")) {
				// Add the table name and fieldname(indextype) used to join idnex.
				for (String i : besttp.getIndexUsedFromJoin().keySet()) {
					queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>()).add(i); // Add the table name to joinindex
					queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>()).add(besttp.getIndexUsedFromJoin().get(i)); // Add the fieldname(indextype)
				}
			}
			tableplanners.remove(besttp);
		}
		return bestplan;
	}

	
	/**
	 * Compare all the different plans using the sum of their respective
	 * blockAccessed and recordsOutput. Then we get the lowest result and
	 * return the plan that acheived the lowest result
	 * 
	 * @param index - Index-Nested Loop Plan
	 * @param sortmerge - Sort-Merge Plan
	 * @param nested - Nested- Loop Plan
	 * @param hash - Grace Hash join
	 * @param tp - Table Planner
	 * @return the plan that yields the lowest blockAccessed + recordsOutput.
	 */
	private Plan compare(Plan index, Plan sortmerge, Plan nested, Plan hash, TablePlanner tp) {
		int indexblocks = Integer.MAX_VALUE, sortblocks = Integer.MAX_VALUE, 
				nestedblocks = Integer.MAX_VALUE, hashblocks = Integer.MAX_VALUE;
		if (index != null)
			indexblocks = index.blocksAccessed() + index.recordsOutput();
		
		if (sortmerge != null) 
			sortblocks = sortmerge.blocksAccessed() + sortmerge.recordsOutput();
		
		if (nested != null)
			nestedblocks = nested.blocksAccessed() + nested.recordsOutput();
		
		if (hash != null)
			hashblocks = hash.blocksAccessed() + hash.recordsOutput();
		
		List<Integer> lowestJoinBlocks = new ArrayList<>(Arrays.asList(indexblocks, sortblocks, nestedblocks, hashblocks));
		List<Plan> lowestJoinPlan = new ArrayList<>(Arrays.asList(index, sortmerge, nested, hash));
		int lowestIndex = lowestJoinBlocks.indexOf(Collections.min(lowestJoinBlocks)); // Get the minimum results from all plans
		
		// Add the lowest plan algorithm to the queryPlan
		if(lowestIndex == 0) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("IndexBasedJoin with " + tp.myplan.tblname);
			// add table w/ joinindex
		} else if(lowestIndex == 1) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("SortMergeJoin with " + tp.myplan.tblname);
		} else if(lowestIndex == 2) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("NestedLoopsJoin with " + tp.myplan.tblname);
		} else {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("HashJoin with " + tp.myplan.tblname);
		} 
		queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname); // Add the table name
		return lowestJoinPlan.get(lowestIndex);
	}

	/**
	 * This method iterates through all table planners
	 * and use ProductPlanJoin to join the tables and get the
	 * best plan with the lowest recordsOutput.
	 * 
	 * @param current the specified plan
	 * @return the lowest plan using ProductPlanJoin
	 */
	private Plan getLowestProductPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeProductPlan(current);
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("ProductPlanJoin");
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
	}

	/**
	 * 
	 * @param p the specified plan
	 */
	public void setPlanner(Planner p) {
		// for use in planning views, which
		// for simplicity this code doesn't do.
	}
}
