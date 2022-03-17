package simpledb.parse;

import java.util.*;

import simpledb.materialize.*;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
	private Lexer lex;
	private List<String> aggOrder = new ArrayList<>();

	private List<AggregationFn> aggFields = new ArrayList<>();

	public Parser(String s) {
		lex = new Lexer(s);
	}

// Methods for parsing predicates, terms, expressions, constants, and fields

	
	/**
	 * Returns the field name
	 * @return field name
	 */
	public String field() {
		return lex.eatId();
	}

	/**
	 * Returns the constant value which can be int or string
	 * @return constant value which can be int or string
	 */
	public Constant constant() {
		if (lex.matchStringConstant())
			return new Constant(lex.eatStringConstant());
		else
			return new Constant(lex.eatIntConstant());
	}

	/**
	 * Returns either constant value or field
	 * @return either constant value or field
	 */
	public Expression expression() {
		if (lex.matchId())
			return new Expression(field());
		else
			return new Expression(constant());
	}

	/**
	 * Split the predicate to 
	 * lhs = field
	 * operator
	 * rhs = field or constant
	 * 
	 * @return Term
	 */
	public Term term() {
		Expression lhs = expression();
		String opr = lex.eatOpr();
		Expression rhs = expression();
		return new Term(lhs, rhs, opr);
	}

	/**
	 * Return the predicate
	 * @return the predicate
	 */
	public Predicate predicate() {
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}

// Methods for parsing queries

	/**
	 * Method to decode the query, get the relevant fields and predicate
	 * and add to the relevant lists
	 * 
	 * @return QueryData with the relevant lists
	 */
	public QueryData query() throws BadSyntaxException {
		List<List<String>> sortFields = null;
		List<String> groupList = null;
		lex.eatKeyword("select");

		//Check for distinct
		boolean isDistinct = false;
		if(lex.matchKeyword("distinct")) {
			lex.eatKeyword("distinct");
			isDistinct = true;
		}
		
		List<String> fields = selectList();		
		
		lex.eatKeyword("from");
		Collection<String> tables = tableList();
		Predicate pred = new Predicate();

		//Check for where statement
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate(); //Store predicates (Join and select) into pred.
		}

		//Check for group by
		if (lex.matchKeyword("group")) {
			lex.eatKeyword("group");
			lex.eatKeyword("by");
			groupList = groupList(); //Store fields to be group by into groupList
		}

		//Check for order by
		if (lex.matchKeyword("order")) {
			lex.eatKeyword("order");
			lex.eatKeyword("by");
			sortFields = sortList(); //Store list of fields (index 0) and list of order by type (index 1). Eg. [[majorid, sname], [asc, desc]]
		}

		return new QueryData(fields, tables, pred, sortFields, aggFields, aggOrder, groupList, isDistinct);
	}

	
	/**
	 * Method to add corresponding aggregate functions to aggFields if exists and 
	 * add selection fields to array list L.
	 * While there are more fields (checking comma), repeat the above.
	 * 
	 * @return L the array list of selection fields
	 */
	private List<String> selectList() {
	    List<String> L = new ArrayList<String>();
	    
	    //Check for aggregate function
	    if (lex.matchAggregate()) {
	      String aggFn = lex.eatAggregate();
	      // list of aggregation functions

	      lex.eatDelim('(');
	      String fldname = field();
	      L.add(fldname);
	      lex.eatDelim(')');
	      switch (aggFn) {
	        case "min": {
	          aggFields.add(new MinFn(fldname));
	          aggOrder.add(new MinFn(fldname).fieldName());
	          break;
	        }
	        case "max": {
	          aggFields.add(new MaxFn(fldname));
	          aggOrder.add(new MaxFn(fldname).fieldName());
	          break;
	        }
	        case "sum": {
	          aggFields.add(new SumFn(fldname));
	          aggOrder.add(new SumFn(fldname).fieldName());
	          break;
	        }
	        case "count": {
	          aggFields.add(new CountFn(fldname));
	          aggOrder.add(new CountFn(fldname).fieldName());
	          break;
	        }
	        case "avg": {
	          aggFields.add(new AvgFn(fldname));
	          aggOrder.add(new AvgFn(fldname).fieldName());
	          break;
	        }
	      }

	    } else {
	      String fldname = field();
	      aggOrder.add(fldname);
	      L.add(fldname);
	      
	    }
	    
	    //While there are more fields, do the same as above.
	    while (lex.matchDelim(',')) {
	      lex.eatDelim(',');
	      if (lex.matchAggregate()) {
	        String aggFn = lex.eatAggregate();
	        // list of aggregation functions

	        lex.eatDelim('(');
	        String fldname = field();
	        if (!L.contains(fldname)) {
	          L.add(fldname);
	        }
	        lex.eatDelim(')');
	        switch (aggFn) {
	        case "min": {
	          aggFields.add(new MinFn(fldname));
	          aggOrder.add(new MinFn(fldname).fieldName());
	          break;
	        }
	        case "max": {
	          aggFields.add(new MaxFn(fldname));
	          aggOrder.add(new MaxFn(fldname).fieldName());
	          break;
	        }
	        case "sum": {
	          aggFields.add(new SumFn(fldname));
	          aggOrder.add(new SumFn(fldname).fieldName());
	          break;
	        }
	        case "count": {
	          aggFields.add(new CountFn(fldname));
	          aggOrder.add(new CountFn(fldname).fieldName());
	          break;
	        }
	        case "avg": {
	          aggFields.add(new AvgFn(fldname));
	          aggOrder.add(new AvgFn(fldname).fieldName());
	          break;
	        }
	      }

	    } else {
	      String fldname = field();
	      aggOrder.add(fldname);
	      L.add(fldname);
	      
	    }
	    }
	    return L;
	  }

	/**
	 * Method to add corresponding table name to the list L.
	 * 
	 * @return L the array list of selection fields
	 */
	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

	/**
	 * Method to parse out sorting clauses proceeding the order by keyword While there are
	 * more fields to be sorted by (check for commas) it will keep looking for more fields to add 
	 * into the two temporary lists
	 * 
	 * @return The list of list of strings containing the fields and order to be
	 *         sorted in
	 */
	private List<List<String>> sortList() {
		List<List<String>> L = new ArrayList<List<String>>();

		String id = field();
		String order = "asc"; //default ordering is asc
		if (lex.matchKeyword("desc")) {
			lex.eatKeyword("desc");
			order = "desc";
		} else if (lex.matchKeyword("asc")) {
			lex.eatKeyword("asc");
			order = "asc";
		}

		//Index position of fieldList and orderList will match to get fields and order type.
		List<String> fieldList = new ArrayList<String>(); //Store fields to be ordered.
		List<String> orderList = new ArrayList<String>(); //Store order type of corresponding fields in fieldList.
		fieldList.add(id);
		orderList.add(order);

		// while there are subsequent order by fields
		while (lex.matchDelim(',')) {
			lex.eatDelim(',');
			id = field();
			order = "asc";
			if (lex.matchKeyword("desc")) {
				lex.eatKeyword("desc");
				order = "desc";
			} else if (lex.matchKeyword("asc")) {
				lex.eatKeyword("asc");
				order = "asc";
			}

			fieldList.add(id);
			orderList.add(order);
		}
		L.add(fieldList);
		L.add(orderList);
		return L;
	}

	/**
	 * Method to parse out group by clauses.
	 * 
	 * @return The list of list of strings containing the fields to be
	 *         group by
	 */
	private List<String> groupList() {

		List<String> L = new ArrayList<String>();

		L.add(field());

		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

// Methods for parsing the various update commands
	
	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}

// Method for parsing delete commands

	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new DeleteData(tblname, pred);
	}

// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

// Method for parsing modify commands

	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new ModifyData(tblname, fldname, newval, pred);
	}

// Method for parsing create table commands

	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = field();
		return fieldType(fldname);
	}

	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		} else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		}
		return schema;
	}

// Method for parsing create view commands

	public CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = query();
		return new CreateViewData(viewname, qd);
	}

//  Method for parsing create index commands

	
	/**
	 * Method to create index. 
	 * SQL Query format Create index "index_name" on "table name" ("attribute") using "index type"
	 * 
	 * @return CreateIndexData class
	 */
	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');
		lex.eatKeyword("using");
		String indexType = lex.eatIndex();
		return new CreateIndexData(idxname, tblname, fldname, indexType);
	}
}
