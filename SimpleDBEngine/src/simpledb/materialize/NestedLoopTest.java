package simpledb.materialize;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.query.*;

public class NestedLoopTest {

	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();
		
		// Get plans for the Student and Enroll tables
		
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);
		
		
		useNestedLoopManually(tx, studentplan, enrollplan, "sid", "studentid");
		useNestedLoopScan(tx, studentplan, enrollplan, "sid", "studentid");
		
		tx.commit();
	}
		
	private static void useNestedLoopManually(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		// Open scans on the tables.
		Scan s1 = p1.open();
		Scan s2 = p2.open();

		// Loop through s1 records. For each value of the join field, 
		// find the matching s2 records.
		while (s1.next()) {
			Constant c = s1.getVal(fldname1);
			while (s2.next()) {
				Constant d = s2.getVal(fldname2);
				if (c.equals(d))
					System.out.println(s1.getVal("sname") + " " + s2.getString("grade"));
			}
			s2.beforeFirst();
		}
		System.out.println();
		s1.close();
		s2.close();
	}

	private static void useNestedLoopScan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		// Open an nested join scan on the table.
		Plan nestedLoopPlan = new NestedLoopPlan(tx, p1, p2, fldname1, fldname2, "=");
		Scan s = nestedLoopPlan.open();

		while (s.next()) {
			System.out.println(s.getVal("sname") + " " + s.getString("grade"));
		}
		s.close();
	}
}
