package simpledb.plan;

import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;


/**
 * PlanTest1 to test the functionality of the chosen Planner
 * of the database and the plan from createQueryPlan.
 * @author group52
 */
public class PlanTest1 {

	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
	    Transaction tx = db.newTx();
	    Planner planner = db.planner();
	    
	    String qry = "select sname, majorid from student";
	    Plan p = planner.createQueryPlan(qry, tx);
	    Scan s = p.open();
	    while (s.next())
	       System.out.println(s.getString("sname") + " " +
	               s.getInt("majorid")); 
	    s.close();
	    tx.commit();
	}
}

