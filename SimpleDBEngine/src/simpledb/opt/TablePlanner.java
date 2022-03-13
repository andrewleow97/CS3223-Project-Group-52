package simpledb.opt;

import java.util.ArrayList;
import java.util.HashMap;
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
	private TablePlan myplan;
	public Predicate mypred;
	private Schema myschema;
	private Map<String, IndexInfo> indexes;
	private Transaction tx;
	private ArrayList<String> storeIndexSelectPlan = new ArrayList<String>();
	private ArrayList<String> indexUsedJoin = new ArrayList<String>();

	/**
	 * Creates a new table planner. The specified predicate applies to the entire
	 * query. The table planner is responsible for determining which portion of the
	 * predicate is useful to the table, and when indexes are useful.
	 * 
	 * @param tblname the name of the table
	 * @param mypred  the query predicate
	 * @param tx      the calling transaction
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
//		Plan p = myplan;
		if (p == null)
			p = myplan;
		return addSelectPred(p);
	}

	/**
	 * Constructs a join plan of the specified plan and the table. The plan will use
	 * an indexjoin, if possible. (Which means that if an indexselect is also
	 * possible, the indexjoin operator takes precedence.) The method returns null
	 * if no join is possible.
	 * 
	 * @param current the specified plan
	 * @return a join plan of the plan and this table
	 */
	public Plan makeIndexJoinPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		Plan p = makeIndexJoin(current, currsch);

		return p;
	}

	public Plan makeSortMergePlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		Plan p = makeSortMergeJoin(current, currsch);
		
		return p;
	}

	public Plan makeNestedLoopPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);

		if (joinpred == null)
			return null;
		Plan p = makeNestedLoopJoin(current, currsch);
			
		return p;
	}

	public Plan makeHashJoinPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		Plan p = makeHashJoin(current, currsch);

		return p;
	}
	
	public Plan makeDefaultProductPlan(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		
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

	private Plan makeIndexSelect() {
		storeIndexSelectPlan = new ArrayList<String>();
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				storeIndexSelectPlan.add(fldname);
				storeIndexSelectPlan.add(ii.getIndexType());
				System.out.println("index on " + fldname + " used");
				return new IndexSelectPlan(myplan, ii, val);
			} else {
				storeIndexSelectPlan.add(fldname);
				storeIndexSelectPlan.add("empty");
			}
		}
		return null;
	}

	public ArrayList<String> getIndexUsedSelectPlan() {
		return storeIndexSelectPlan;
	}
	
	public ArrayList<String> getIndexUsedFromJoin() {
		return indexUsedJoin;
	}
	
	private Plan makeIndexJoin(Plan current, Schema currsch) {
		System.out.println(indexes.keySet());
		for (String fldname : indexes.keySet()) {
			String outerfield = mypred.equatesWithField(fldname);
//			System.out.println(outerfield);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				indexUsedJoin.add(fldname);
				indexUsedJoin.add(ii.getIndexType());
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	private Plan makeSortMergeJoin(Plan current, Schema currsch) {
		// tx, p1 = current, p2 = myplan, fldname1, fldname2
		String fldname1 = mypred.terms.get(0).LHS();
		String fldname2 = mypred.terms.get(0).RHS();
		if (currsch.hasField(fldname1)) {
			Plan p = new MergeJoinPlan(tx, current, myplan, fldname1, fldname2);

			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		} else if (currsch.hasField(fldname2)) {
			Plan p = new MergeJoinPlan(tx, myplan, current, fldname1, fldname2);
			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		}

		return null;
	}

	private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
		String fldname1 = mypred.terms.get(0).LHS();
		String fldname2 = mypred.terms.get(0).RHS();
		String opr = mypred.terms.get(0).operator();
		
		if (currsch.hasField(fldname1)) {
			Plan p = new NestedLoopPlan(tx, current, myplan, fldname1, fldname2, opr);
			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		} else if (currsch.hasField(fldname2)) {
			
			Plan p = new NestedLoopPlan(tx, myplan, current, fldname1, fldname2, opr);
			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		}
		return null;
	}

	private Plan makeHashJoin(Plan current, Schema currsch) {
		String fldname1 = mypred.terms.get(0).LHS();
		String fldname2 = mypred.terms.get(0).RHS();
		if (currsch.hasField(fldname1)) {
			HashPartitionPlan currpartition = new HashPartitionPlan(tx, current, fldname1);
			HashPartitionPlan mypartition = new HashPartitionPlan(tx, myplan, fldname2);
			Plan p = new HashJoinPlan(tx, currpartition, mypartition, fldname1, fldname2);
			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		} else if (currsch.hasField(fldname2)) {
			HashPartitionPlan currpartition = new HashPartitionPlan(tx, current, fldname2);
			HashPartitionPlan mypartition = new HashPartitionPlan(tx, myplan, fldname1);
			Plan p = new HashJoinPlan(tx, currpartition, mypartition, fldname2, fldname1);
			p = addSelectPred(p);
			return addJoinPred(p, currsch);
		}

		return null;
	}

	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinPred(p, currsch);
	}

	private Plan addSelectPred(Plan p) {
		Predicate selectpred = mypred.selectSubPred(myschema);
		if (selectpred != null)
			return new SelectPlan(p, selectpred);
		else
			return p;
	}

	private Plan addJoinPred(Plan p, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		if (joinpred != null)
			return new SelectPlan(p, joinpred);
		else
			return p;
	}
}
