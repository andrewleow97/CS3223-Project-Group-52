package simpledb.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.HashPartitionPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.materialize.NestedLoopPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
	public TablePlan myplan;
	public Predicate mypred;
	private Schema myschema;
	private Map<String, IndexInfo> indexes;
	private Transaction tx;
	private HashMap<String, String> storeIndexSelectPlan = new HashMap<>();
	private HashMap<String, String> indexUsedJoin = new HashMap<>();

	/**
	 * Creates a new table planner. The specified predicate applies to the entire
	 * query. The table planner is responsible for determining which portion of the
	 * predicate is useful to the table, choose which join algorithm is called, and when indexes are useful.
	 * 
	 * @param tblname the name of the table
	 * @param mypred  the query predicate
	 * @param tx      the calling transaction
	 * @param mdm     the metadata manager to obtain index information
	 */
	public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
		this.mypred = mypred;
		this.tx = tx;
		myplan = new TablePlan(tx, tblname, mdm);
		myschema = myplan.schema();
		indexes = mdm.getIndexInfo(tblname, tx);
	}

	/**
	 * Constructs a select plan for the table. The plan will use an indexselect, if
	 * possible.
	 * 
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelect();
		if (p == null)
			p = myplan;
		return addSelectPred(p);
	}

	/**
	 * Constructs an index join plan of the specified plan and the table, if possible. 
	 * (Which means that if an indexselect is also
	 * possible, the indexjoin operator takes precedence.) 
	 * The method returns null if no join is possible.
	 * 
	 * @param current the specified plan
	 * @return an index join plan of the current plan and this table
	 */
	public Plan makeIndexJoinPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		Plan p = makeIndexJoin(current, currsch);

		return p;
	}

	/**
	 * Constructs a sort merge join plan of the specified plan and the table, if possible. 
	 * The method returns null if no join is possible.
	 * 
	 * @param current the specified plan
	 * @return a sort merge join plan of the current plan and this table
	 */
	public Plan makeSortMergePlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		Plan p = makeSortMergeJoin(current, currsch);
		
		return p;
	}

	
	/**
	 * Constructs a nested loop join plan of the specified plan and the table, if possible. 
	 * The method returns null if no join is possible.
	 * 
	 * @param current the specified plan
	 * @return a nested loop join plan of the current plan and this table
	 */
	public Plan makeNestedLoopPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);

		if (joinpred == null)
			return null;
		Plan p = makeNestedLoopJoin(current, currsch);
			
		return p;
	}

	/**
	 * Constructs a grace hash join plan of the specified plan and the table, if possible. 
	 * The method returns null if no join is possible.
	 * 
	 * @param current the specified plan
	 * @return a grace hash join plan of the current plan and this table
	 */
	public Plan makeHashJoinPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		
		if (joinpred == null)
			return null;
		Plan p = makeHashJoin(current, currsch);

		return p;
	}
	
	/**
	 * Constructs a cross product join plan of the specified plan and the table.
	 * 
	 * @param current the specified plan
	 * @return a cross product join plan of the current plan and this table
	 */
	public Plan makeDefaultProductPlan(Plan current) {
		Schema currsch = current.schema();
		Plan p = makeProductJoin(current, currsch);
		return p;
	}

	/**
	 * Constructs a product plan of the specified plan and this table.
	 * 
	 * @param current the specified plan
	 * @return a product plan of the specified plan and this table
	 */
	public Plan makeProductPlan(Plan current) {
		Plan p = addSelectPred(myplan);
		return new MultibufferProductPlan(tx, current, p);
	}

	/**
	 * Constructs an index selection plan of the selection field and this table.
	 * The function will check if the selection predicate is a range query, and if the index used is a hash
	 * As hash indexes are incompatible with range queries, it will choose to not use an index selection
	 * Otherwise if a btree hash is present on the field, it will attempt to use that.
	 * The function also stores the index used for selection in storeIndexSelectPlan for use in the query plan.
	 * 
	 * @return an index select plan of the specified plan and this table if possible, otherwise return null
	 */
	private Plan makeIndexSelect() {
		storeIndexSelectPlan = new HashMap<String, String>();
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				boolean isHashIndex = ii.getIndexType().contains("hash");
				boolean isEqualOpr = true;
				for (Term term: mypred.terms) {
					// if either lhs or rhs has the field, or if the operator is not equality
					if((term.LHS().equals(fldname) || term.RHS().equals(fldname)) && !term.operator().equals("=")) {
						isEqualOpr = false;
						break;
					}
				}
				if (!isEqualOpr && isHashIndex) {
					return null;
				}
				storeIndexSelectPlan.put(myplan.tblname, fldname + "(" + ii.getIndexType() + ")");
				System.out.println("index on " + fldname + " used");
				String operator = mypred.getSelectOperator(fldname);
				return new IndexSelectPlan(myplan, ii, val, operator);
			}
		}
		return null;
	}

	/**
	 * Returns the index used for selection and its corresponding fieldname.
	 * This will be used in the query plan.
	 * 
	 * @return a hash table of the index used for selection
	 */
	public HashMap<String, String> getIndexUsedSelectPlan() {
		return storeIndexSelectPlan;
	}
	
	/**
	 * Returns the index used for join and its corresponding fieldname.
	 * This will be used in the query plan.
	 * 
	 * @return a hash table of the index used for join
	 */
	public HashMap<String, String> getIndexUsedFromJoin() {
		return indexUsedJoin;
	}
	
	/**
	 * Attempts to return an index join plan on the current plan and myplan
	 * The function will check if the outerfield matches any fields in the index
	 * If so, and if the operator is for equality jion, then return the index join plan
	 * Otherwise, return null.
	 * 
	 * @param current the specified plan
	 * @param currsch the schema of the specified plan
	 * @return an index join plan of the current plan and myplan, using the specified index and outer join field
	 */
	private Plan makeIndexJoin(Plan current, Schema currsch) {
		for (String fldname : indexes.keySet()) {
			String outerfield = mypred.equatesWithField(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				
				String operator = mypred.getJoinOperator(fldname);
				indexUsedJoin.put(myplan.tblname, fldname + "(" + ii.getIndexType() + ")");
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, operator);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	/**
	 * Attempts to return a sort merge join plan on the current plan and myplan
	 * The function will check if the LHS and RHS fields match any fieldnames in the current plan and my plan
	 * If so, then return the sort merge join plan with the corresponding inner and outer tables.
	 * Otherwise, return null.
	 * 
	 * @param current the specified plan
	 * @param currsch the schema of the specified plan
	 * @return a sort merge join plan of the current plan and myplan, using the specified inner and outer tables and join fields
	 */
	private Plan makeSortMergeJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();
			// selecting inner and outer tables
			if (currsch.hasField(fldname1) && myplan.schema().hasField(fldname2)) {
				Plan p = new MergeJoinPlan(tx, current, myplan, fldname1, fldname2);
	
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			} else if (currsch.hasField(fldname2) && myplan.schema().hasField(fldname1)) {
				Plan p = new MergeJoinPlan(tx, myplan, current, fldname1, fldname2);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	/**
	 * Attempts to return a nested loop join plan on the current plan and myplan
	 * The function will check if the LHS and RHS fields match any fieldnames in the current plan and my plan
	 * If so, then return the sort merge join plan with the corresponding inner and outer tables
	 * as well as the operator for non-equality join support for nested loop join.
	 * Otherwise, return null.
	 * 
	 * @param current the specified plan
	 * @param currsch the schema of the specified plan
	 * @return a nested loop join plan of the current plan and myplan, using the specified inner and outer tables, join fields and operator
	 */
	private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();
			String opr = t.operator();
			// selecting inner and outer tables
			if (currsch.hasField(fldname1) && myplan.schema().hasField(fldname2) ) {

				Plan p = new NestedLoopPlan(tx, current, myplan, fldname1, fldname2, opr);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			} else if (currsch.hasField(fldname2) && myplan.schema().hasField(fldname1)) {

				Plan p = new NestedLoopPlan(tx, myplan, current, fldname1, fldname2, opr);
				
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	/**
	 * Attempts to return a hash join plan on the current plan and myplan
	 * The function will check if the LHS and RHS fields match any fieldnames in the current plan and my plan
	 * If so, then return the hash join plan with the corresponding inner and outer tables.
	 * Otherwise, return null.
	 * 
	 * @param current the specified plan
	 * @param currsch the schema of the specified plan
	 * @return a hash join plan of the current plan and myplan, using the specified inner and outer tables and join fields.
	 */
	private Plan makeHashJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();
			// selecting inner and outer tables
			if (currsch.hasField(fldname1) && myplan.schema().hasField(fldname2)) {
				HashPartitionPlan currpartition = new HashPartitionPlan(tx, current, fldname1);
				HashPartitionPlan mypartition = new HashPartitionPlan(tx, myplan, fldname2);
				Plan p = new HashJoinPlan(tx, currpartition, mypartition, fldname1, fldname2);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);

			} else if (currsch.hasField(fldname2) && myplan.schema().hasField(fldname1)) {
				HashPartitionPlan currpartition = new HashPartitionPlan(tx, current, fldname2);
				HashPartitionPlan mypartition = new HashPartitionPlan(tx, myplan, fldname1);
				Plan p = new HashJoinPlan(tx, currpartition, mypartition, fldname2, fldname1);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}

		return null;
	}

	/**
	 * Returns a product join plan on the current plan and myplan
	 * 
	 * @param current the specified plan
	 * @param currsch the schema of the specified plan
	 * @return a product join plan of the current plan and myplan.
	 */
	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinPred(p, currsch);
	}

	/**
	 * Attempts to return the select plan used to select tuples based on the select predicate.
	 * If there is no select predicate, return the original plan instead.
	 * 
	 * @param p the specified plan
	 * @return a select plan of the current plan on the select predicate.
	 */
	private Plan addSelectPred(Plan p) {
		Predicate selectpred = mypred.selectSubPred(myschema);
		if (selectpred != null)
			return new SelectPlan(p, selectpred);
		else
			return p;
	}

	/**
	 * Attempts to return the select plan used to select tuples based on the join predicate.
	 * If there is no select predicate, return the original plan instead.
	 * 
	 * @param p the specified plan
	 * @return a select plan of the current plan on the select predicate.
	 */
	private Plan addJoinPred(Plan p, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		if (joinpred != null)
			return new SelectPlan(p, joinpred);
		else
			return p;
	}
	
	/**
	 * Returns the join predicate used in the current plan for the query plan output.
	 * 
	 * @param current the specified plan
	 * @return joinpred the predicate used to join the current plan, used for query plan output
	 */
	public Predicate returnJoinPredicate(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		return joinpred;
	}
}
