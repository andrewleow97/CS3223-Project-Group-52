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
		for(String table: data.tables()) {
			queryPlan.computeIfAbsent("table", k -> new ArrayList<>()).add(table);
		}
		queryPlan.computeIfAbsent("spred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("jpred", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("join", k -> new ArrayList<>());
		queryPlan.computeIfAbsent("index", k -> new ArrayList<>());

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
		System.out.println(queryPlan.toString());

		// Step 4. Project on the field names and return
		currentplan = new ProjectPlan(currentplan, data.fields());

		// Step 5. Group by
		if (data.aggFields() != null && data.groupList() == null && data.aggFields().size() > 0) {
			currentplan = new AggregatePlan(tx, currentplan, data.aggFields(), data.fields());
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
				//If there is no index used at all
				//TODO: Must we include a condition that if the join is not IndexJoin, we do not put anything for the index?
				try {
					String fldname = queryPlan.get("index").get(i*2);
					String indexType = queryPlan.get("index").get((i*2)+1);
					if(indexType != "empty") {
						s += "(" + indexType + " index on " + table + ")";
					} else {
						s += "(scan " + table + ")";
					}
				} catch (IndexOutOfBoundsException e) {
					s += "(scan " + table + ")";
				}
				
				//if is not the last table
				if (i != queryPlan.get("table").size()-1) {
					s += " " + queryPlan.get("join").get(0) + " ";
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
			for(int i = 0; i < queryPlan.get("table").size(); i++) {
				if(!queryPlan.get("index").isEmpty()) {
					String fldname = queryPlan.get("index").get(i*2);
					String indexType = queryPlan.get("index").get((i*2)+1);
//					System.out.println(indexType);

					if(indexType != "empty") {
						s += "(" + indexType + " index on " + fldname + ")";
					} else {
						s += "(scan " + queryPlan.get("table").get(i) + ")";
					}
					try {
						s += queryPlan.get("join").get(i);
					} catch (IndexOutOfBoundsException e) {
						continue;
					}
					if(!queryPlan.get("join").isEmpty()) {
						s += queryPlan.get("join").get(0);
					}
				} else {
					s += "(scan " + queryPlan.get("table").get(i) + ")";
					try {
						s += queryPlan.get("join").get(i);
					} catch (IndexOutOfBoundsException e) {
						continue;
					}
				}
			}
		}
		
		System.out.println(s);
	}
	
	private Plan getLowestSelectPlan() {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeSelectPlan();
			ArrayList<String> indexUsed = new ArrayList<>();
			indexUsed = tp.getIndexUsedSelectPlan();
//			System.out.println(indexUsed);
			if (indexUsed.size() != 0) {
				for (int i = 0; i < indexUsed.size()/2; i++) {
					
					queryPlan.computeIfAbsent("index", k -> new ArrayList<>()).add(indexUsed.get(i*2).toString());
					queryPlan.computeIfAbsent("index", k -> new ArrayList<>()).add(indexUsed.get(i*2 + 1).toString());
				}
			}
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				//If the index is not in the join pred. cause we dw it to remove, wanna have it for index join.
				//if(!queryPlan.get("jpred").get(0).contains(indexUsed.get(0)) ) {
//					System.out.println(indexUsed);
					besttp = tp;
					bestplan = plan;
				//}
			}
		}
//		System.out.println(besttp.getIndexUsed());
		
		tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			
			Plan indexPlan = tp.makeIndexJoinPlan(current);
			//If there is an index used.
			if (indexPlan != null) {
				for(int i = 0; i < queryPlan.get("index").size(); i++) {
					
					if (tp.getIndexUsedFromJoin().get(0) == queryPlan.get("index").get(i)) {
						queryPlan.get("index").set(i+1, tp.getIndexUsedFromJoin().get(1));
					}
				}
			}
			Plan sortMergePlan = tp.makeSortMergePlan(current);
			Plan nestedLoopPlan = tp.makeNestedLoopPlan(current);
			Plan hashJoinPlan = tp.makeHashJoinPlan(current);
			System.out.println(queryPlan);
			if(sortMergePlan == null && nestedLoopPlan == null && hashJoinPlan == null) {
				Plan productPlan = tp.makeDefaultProductPlan(current);
				bestplan = productPlan;
				queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("ProductPlanJoin");
			} else {
				// TODO: If all plans are null, default the best plan to product plan.
				if (tp.mypred.terms.size() > 0 && tp.mypred.terms.get(0).operator().equals("=")) {
					bestplan = compare(indexPlan, sortMergePlan, nestedLoopPlan, hashJoinPlan);
				}
				
				else {// if non-equi join{ bestplan = nestedloopplan}
					bestplan = nestedLoopPlan;
					queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("NestedLoopsJoin");
				}
			}
			
			if (bestplan != null)
				besttp = tp;
		}
		if (bestplan != null)
			tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan compare(Plan index, Plan sortmerge, Plan nested, Plan hash) {
		int indexblocks = Integer.MAX_VALUE, sortblocks = Integer.MAX_VALUE, nestedblocks = Integer.MAX_VALUE, hashblocks = Integer.MAX_VALUE;
		if (index != null)
			indexblocks = index.blocksAccessed() + index.recordsOutput();
		if (sortmerge != null) {
			sortblocks = sortmerge.blocksAccessed() + sortmerge.recordsOutput();
		}
		if (nested != null)
			nestedblocks = nested.blocksAccessed() + nested.recordsOutput();
		if (hash != null)
			hashblocks = hash.blocksAccessed() + hash.recordsOutput();
		
		List<Integer> lowestJoinBlocks = new ArrayList<>(Arrays.asList(indexblocks, sortblocks, nestedblocks, hashblocks));
		List<Plan> lowestJoinPlan = new ArrayList<>(Arrays.asList(index, sortmerge, nested, hash));
		int lowestIndex = lowestJoinBlocks.indexOf(Collections.min(lowestJoinBlocks));
		if(lowestIndex == 0) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("IndexBasedJoin");
		} else if(lowestIndex == 1) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("SortMergeJoin");
		} else if(lowestIndex == 2) {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("NestedLoopsJoin");
		} else {
			queryPlan.computeIfAbsent("join", k -> new ArrayList<>()).add("HashJoin");
		}
		
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
