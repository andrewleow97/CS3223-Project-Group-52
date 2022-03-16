package simpledb.materialize;

import java.util.Arrays;
import java.util.Map;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.query.*;
import simpledb.index.*;
import simpledb.materialize.*;

// Find the grades of all students.

public class SortMergeTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();
      
		// Get plans for the Student and Enroll tables
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);
		
		Plan p1 = new SortPlan(tx, studentplan, Arrays.asList("sid"));
		Plan p2 = new SortPlan(tx, enrollplan, Arrays.asList("studentid"));
		// Two different ways to use the index in simpledb:
		useMergeJoinManually(tx, p1, p2, "sid", "studentid");		
		useMergeScan(tx, p1, p2, "sid", "studentid");

		tx.commit();
	}

	private static void useMergeJoinManually(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		// Open scans on the tables.
		Scan s1 = p1.open();
		SortScan s2 = (SortScan) p2.open();  //must be a sort scan

		// Loop through s1 records. For each value of the join field, 
		// find the matching s2 records.
		while (s1.next()) {
			Constant c = s1.getVal(fldname1);
			s2.beforeFirst();
			while (s2.next()) {
				Constant d = s2.getVal(fldname2);
				if (c.equals(d))
					System.out.println(s1.getVal("sname") + " " + s2.getString("grade"));
			}
		}
		System.out.println();
		s1.close();
		s2.close();
	}

	private static void useMergeScan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		// Open an index join scan on the table.
		Plan mergePlan = new MergeJoinPlan(tx, p1, p2, fldname1, fldname2);
		Scan s = mergePlan.open();

		while (s.next()) {
			System.out.println(s.getVal("sname") + " " + s.getString("grade"));
		}
		s.close();
	}
}
