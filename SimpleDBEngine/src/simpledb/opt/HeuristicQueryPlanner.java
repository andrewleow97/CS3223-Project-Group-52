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
	 */
	public Plan createPlan(QueryData data, Transaction tx) {
		queryPlan = new HashMap<>();
		queryPlan.computeIfAbsent("spred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("jpred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("join", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>());
		for(String d : data.fields()) {
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
//		System.out.println(queryPlan.toString());
		
		// Step 4. Project on the field names and return
		currentplan = new ProjectPlan(currentplan, data.fields());

		// Step 5. Group by
		if (data.aggFields() != null && data.groupList() == null && data.aggFields().size() > 0) {
			currentplan = new AggregatePlan(tx, currentplan, data.aggFields());
		}

		if (data.groupList() != null && data.aggFields() != null) {
			currentplan = new GroupByPlan(tx, currentplan, data.groupList(), data.aggFields());
		}
		
		getQueryPlan();
		
		
		// Step 6. Sort the final plan node w/ distinct support
		if (data.sortFields() == null || data.sortFields().isEmpty()) {
			if (data.isDistinct() != false) {
				// sort plan first
				
				currentplan = new SortPlan(tx, currentplan, data.fields());
				// eliminate duplicates
				currentplan = new DistinctPlan(tx, currentplan, data.fields());
			}
			return currentplan;
		}

		else {
			// sort plan first
			if (data.isDistinct() != false) {
				// add sorting 
				for (String field : data.fields()) {
					if (!data.sortFields().contains(field)) {
						data.sortFields().add(field);
						data.sortOrder().add("asc");
					}
				}
				currentplan = new SortPlan(tx, currentplan, data.sortFields(), data.sortOrder());
				// eliminate duplicates
				return new DistinctPlan(tx, currentplan, data.fields());
			} else {
				// just return, no duplicate elimination
				currentplan = new SortPlan(tx, currentplan, data.sortFields(), data.sortOrder());
				return currentplan;
			}
		}

	}

	private void getQueryPlan() {
		int iJoinIndex = 0;
		String s = "";
		s += "select ";
		
		for(String d: queryPlan.get("field")) {
			s += "(" + d + ")";
		}
		
		if (queryPlan.get("spred").size() != 0) {
			for(String sp : queryPlan.get("spred")) {
				s += "(" + sp + ")";
			}
		}
		if (queryPlan.get("jpred").size() != 0) {
			s += " [";
			
			for(int i = 0; i < queryPlan.get("table").size(); i++) {
				String table = queryPlan.get("table").get(i);
				
				if (iJoinIndex > queryPlan.get("join").size()-1)
					iJoinIndex--;
		        
				if (queryPlan.get("join").get(iJoinIndex).contains("Index")) {
		        	
		          int indexAtJoinIndex = queryPlan.get("joinindex").indexOf(table);
		          
		          // if joinindex does not contain index on current table, freeze ijoinindex and move to next table
		          if (indexAtJoinIndex < 0) {
		            s += "(" + table + ")";
		            s += " " + queryPlan.get("join").get(i) + " ";
		            continue;
		          }
		            
		          // joinindex contains current table, get indextype used
		           else {
			          String joinIndexType = queryPlan.get("joinindex").get(indexAtJoinIndex + 1);
			          s += "(" + joinIndexType + " on " + table + ")";
			          if (i != queryPlan.get("table").size()-1)
			        	  s += " " + queryPlan.get("join").get(i) + " ";       
			          iJoinIndex++;
			          continue;
		          }
		          
		        // table @ iJoinIndex is not an index join, perform normal join and move to next join
		        } else {
		          s += "(" + table + ")";
		          if (i != queryPlan.get("table").size()-1) {
		            s += " " + queryPlan.get("join").get(i) + " ";
		          }
		          iJoinIndex++;
		        }
			}
			s += "] ";
			
			//add join pred
			for(String jp : queryPlan.get("jpred")) {
				s += "(" + jp + ")";
			}
		}
		//Else there is no join predicate
		//select majorid, studentid from enroll, student where majorid > 10;
		else {
			s += "[";
			for(int i = 0; i < queryPlan.get("table").size(); i++) {
				if(!queryPlan.get("selectindex").isEmpty()) {
					String fldname;
					try {
						fldname = queryPlan.get("selectindex").get(i*2);
					} catch (IndexOutOfBoundsException e) {
						fldname = null;
					}
					if(fldname != null) {
						s += "(" + queryPlan.get("selectindex").get(i*2 + 1) + " index on " + queryPlan.get("table").get(i) + ")";
					} else {
						s += "(" + queryPlan.get("table").get(i) + ")";
					}
					try {
						s += queryPlan.get("join").get(i);
					} catch (IndexOutOfBoundsException e) {
						continue;
					}
					if(!queryPlan.get("join").isEmpty()) {
						s += queryPlan.get("join").get(i);
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
		HashMap<String, String> indexUsed = new HashMap<>();
		indexUsed = besttp.getIndexUsedSelectPlan();
		for (String i : indexUsed.keySet()) {
			queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>()).add(i);
			queryPlan.computeIfAbsent("selectindex", k -> new ArrayList<>()).add(indexUsed.get(i));
		}

		queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(besttp.myplan.tblname);
		
		tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		Plan currentplan = null;
		for (TablePlanner tp : tableplanners) {
			
			Plan indexPlan = tp.makeIndexJoinPlan(current);
			Plan sortMergePlan = tp.makeSortMergePlan(current);
			Plan nestedLoopPlan = tp.makeNestedLoopPlan(current);
			Plan hashJoinPlan = tp.makeHashJoinPlan(current);
			Plan productPlan = tp.makeDefaultProductPlan(current);
			
			Predicate joinPred = tp.returnJoinPredicate(current);
			// equi join
			if (joinPred != null && joinPred.terms.get(0).operator().equals("=")) {
				currentplan = compare(indexPlan, sortMergePlan, nestedLoopPlan, hashJoinPlan, tp);

			}	
			// non-equi join
			else if (joinPred != null){// if non-equi join{ bestplan = nestedloopplan}
				currentplan = nestedLoopPlan;
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("NestedLoopsJoin with " + tp.myplan.tblname);
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname);
			}
			// no join predicate
			else {
				currentplan = tp.makeDefaultProductPlan(current);
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("ProductPlanJoin with " + tp.myplan.tblname);
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname);
			}

			
			if (currentplan != null) {
				if (bestplan == null) {
					bestplan = currentplan;
					besttp = tp;
				} else if ((bestplan.blocksAccessed() + bestplan.recordsOutput()) > (currentplan.blocksAccessed() + currentplan.recordsOutput())) {
					bestplan = currentplan;
					besttp = tp;
					//update queryplan
					queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-2);
					queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-2);
					
				} else {
					queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-1);
					queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-1);
				}
				
			} else {
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).remove(queryPlan.get("join").size()-1);
				queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).remove(queryPlan.get("table").size()-1);
			}
		}
		if (bestplan != null) {// update queryplan (move all the add join to queryplan logic here)
			
			if (queryPlan.containsKey("join") && queryPlan.get("join").get(queryPlan.get("join").size()-1).contains("Index")) {
				
				for (String i : besttp.getIndexUsedFromJoin().keySet()) {
					queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>()).add(i);
					queryPlan.computeIfAbsent("joinindex", k -> new ArrayList<>()).add(besttp.getIndexUsedFromJoin().get(i));
				}
			}
			tableplanners.remove(besttp);
		}
		return bestplan;
	}

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
		int lowestIndex = lowestJoinBlocks.indexOf(Collections.min(lowestJoinBlocks));
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
		queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(tp.myplan.tblname);
		return lowestJoinPlan.get(lowestIndex);
	}

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

	public void setPlanner(Planner p) {
		// for use in planning views, which
		// for simplicity this code doesn't do.
	}
}
