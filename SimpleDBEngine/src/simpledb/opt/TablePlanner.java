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
		storeIndexSelectPlan = new HashMap<String, String>();
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				boolean isHashIndex = ii.getIndexType().contains("hash");
				boolean isEqualOpr = true;
				for (Term term: mypred.terms) {
					if((term.LHS().equals(fldname) || term.RHS().equals(fldname)) && !term.operator().equals("=")) {
						isEqualOpr = false;
						break;
					}
				}
				if (!isEqualOpr && isHashIndex) {
//					System.out.println("hash index incompatible with range query");
					return null;
				}
				storeIndexSelectPlan.put(myplan.tblname, fldname + "(" + ii.getIndexType() + ")");
				System.out.println("index on " + fldname + " used");
				String operator = mypred.getSelectOperator(fldname);
				if (operator == null) {
//					System.out.println("operator is null");
				}
				return new IndexSelectPlan(myplan, ii, val, operator);
			}
		}
		return null;
	}

	public HashMap<String, String> getIndexUsedSelectPlan() {
		return storeIndexSelectPlan;
	}
	
	public HashMap<String, String> getIndexUsedFromJoin() {
		return indexUsedJoin;
	}
	
	private Plan makeIndexJoin(Plan current, Schema currsch) {
		for (String fldname : indexes.keySet()) {
			String outerfield = mypred.equatesWithField(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				
				String operator = mypred.getJoinOperator(fldname);
				if (operator == null) {
//					System.out.println("operator is null");
				}
				indexUsedJoin.put(myplan.tblname, fldname + "(" + ii.getIndexType() + ")");
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, operator);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	private Plan makeSortMergeJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();
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

	private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();
			String opr = t.operator();

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

	private Plan makeHashJoin(Plan current, Schema currsch) {
		for (Term t : mypred.terms) {
			String fldname1 = t.LHS();
			String fldname2 = t.RHS();

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
	
	public Predicate returnJoinPredicate(Plan current) {
		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		return joinpred;
	}
}
