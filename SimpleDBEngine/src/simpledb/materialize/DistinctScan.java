package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

public class DistinctScan implements Scan {
	private Scan s1 = null;
	private RecordComparator comp;
	private boolean hasmore1 = false;
	private HashMap<String, Constant>prev = new HashMap<>();
//	private SortScan prev = null;
	private List<String> fields = new ArrayList<>();

	public DistinctScan(Scan s, RecordComparator comp, List<String> fields) {
		this.s1 = (Scan) s;
		this.comp = comp;
		this.fields = fields;
		s1.beforeFirst();
	}

	public void beforeFirst() {
		s1.beforeFirst();
		hasmore1 = s1.next();
	}

	public void updatePrev() {
		for (String field : this.fields) {
			Constant tempval = this.s1.getVal(field);
			prev.put(field, tempval);
		}
	}

	public boolean isDistinct(HashMap<String, Constant> prev, Scan s1) {
		for (String field : this.fields) {
			Constant tempval = this.s1.getVal(field);
			if (prev.get(field).compareTo(tempval) != 0)
				return true;
		}
		return false;
	}
	
	public boolean next() {
		if (prev.isEmpty()) {
			updatePrev();
			hasmore1 = s1.next();
			return true;
		} 
//		if (prev == null) {
//			prev = s1;
//			hasmore1 = s1.next();
//			return true;
//		}
		if (!hasmore1) { // end of scan
			return false;
		}
		while (hasmore1) {
			System.out.println("bef " + s1.getVal("gradyear"));
			System.out.println("bef " + prev.get("gradyear"));
			if (isDistinct(this.prev, this.s1)) { // is distinct
				System.out.println("not duplicate");
//				System.out.println("1 " + s1.getVal("gradyear"));
//				System.out.println("1 " + prev.getVal("gradyear"));
//				prev = s1;
//				System.out.println("2 " + s1.getVal("gradyear"));
//				System.out.println("2 " + prev.getVal("gradyear"));
				updatePrev();
				hasmore1 = s1.next();
				return true;
			} else { // is not distinct
				hasmore1 = s1.next();
				
			}
		}
		return false;
	}


	public int getInt(String fldname) {
		return s1.getInt(fldname);
	}


	public String getString(String fldname) {
		return s1.getString(fldname);
	}


	public Constant getVal(String fldname) {
		return s1.getVal(fldname);
	}


	public boolean hasField(String fldname) {
		return s1.hasField(fldname);
	}


	public void close() {
		s1.close();
	}

}
