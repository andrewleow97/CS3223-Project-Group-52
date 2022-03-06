package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

public class DistinctScan implements Scan {
	private SortScan s1 = null;
	private RecordComparator comp;
	private boolean hasmore1 = false;
	private HashMap<String, Constant>prev = new HashMap<>();
	private List<String> fields = new ArrayList<>();

	public DistinctScan(Scan s, RecordComparator comp, List<String> fields) {
		this.s1 = (SortScan) s;
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
			Constant tempval = this.s1.getValue(field);
			prev.put(field, tempval);
		}
	}

	public boolean isDistinct(HashMap<String, Constant> prev, SortScan s1) {
		boolean hasdiff = false;
		for (String field : this.fields) {
			Constant tempval = s1.getValue(field);
			if (prev.get(field).compareTo(tempval) != 0) {
				hasdiff = true;
			}
		}

		return hasdiff;
	}

	
	public boolean next() {
		if (prev.isEmpty()) {
			updatePrev();

			hasmore1 = s1.next();

			return true;
		} 

		if (!hasmore1) { // end of scan
			return false;
		}
		while (hasmore1 = s1.next()) {
			if (isDistinct(this.prev, this.s1)) { // is distinct
				updatePrev();
				return true;
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
