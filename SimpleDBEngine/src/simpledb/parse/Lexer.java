package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * 
 * @author Edward Sciore
 */
public class Lexer {
	private Collection<String> keywords;
	private Collection<String> comparators;
	private Collection<String> indexes;
	private Collection<String> aggregates;
	private StreamTokenizer tok;

	/**
	 * Creates a new lexical analyzer for SQL statement s.
	 * 
	 * @param s the SQL statement
	 */
	public Lexer(String s) {
		initKeywords();
		initComparators();
		initIndex();
		initAggregates();
		tok = new StreamTokenizer(new StringReader(s));
		tok.ordinaryChar('.'); // disallow "." in identifiers
		tok.wordChars('_', '_'); // allow "_" in identifiers

		tok.wordChars('<', '<'); // allow "<" in identifiers
		tok.wordChars('>', '>'); // allow ">" in identifiers
		tok.wordChars('=', '='); // allow "=" in identifiers
		tok.wordChars('!', '!'); // allow "!" in identifiers

		tok.lowerCaseMode(true); // ids and keywords are converted to lowercase
		nextToken();
	}

//Methods to check the status of the current token

	/**
	 * Returns true if the current token is the specified delimiter character.
	 * 
	 * @param d a character denoting the delimiter
	 * @return true if the delimiter is the current token
	 */
	public boolean matchDelim(char d) {
		return d == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is an integer.
	 * 
	 * @return true if the current token is an integer
	 */
	public boolean matchIntConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER;
	}

	/**
	 * Returns true if the current token is a string.
	 * 
	 * @return true if the current token is a string
	 */
	public boolean matchStringConstant() {
		return '\'' == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is the specified keyword.
	 * 
	 * @param w the keyword string
	 * @return true if that keyword is the current token
	 */
	public boolean matchKeyword(String w) {
		return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
	}

	/**
	 * Returns true if the current token is a legal identifier.
	 * 
	 * @return true if the current token is an identifier
	 */
	public boolean matchId() {
		return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is in the specified comparator list.
	 * 
	 * @param w the comparator string
	 * @return true if that comparator is the current token
	 */
	public boolean matchComparator() {
		return tok.ttype == StreamTokenizer.TT_WORD && comparators.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is in the specified index list.
	 * 
	 * @param w the comparator string
	 * @return true if that index is the current token
	 */
	public boolean matchIndex() {
		return tok.ttype == StreamTokenizer.TT_WORD && indexes.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is in the specified aggregate list.
	 * 
	 * @param w the comparator string
	 * @return true if that aggregate is the current token
	 */
	public boolean matchAggregate() {
		return tok.ttype == StreamTokenizer.TT_WORD && aggregates.contains(tok.sval);
	}

	//Methods to "eat" the current token

	/**
	 * Throws an exception if the current token is not the specified delimiter.
	 * Otherwise, moves to the next token.
	 * 
	 * @param d a character denoting the delimiter
	 */
	public void eatDelim(char d) {
		if (!matchDelim(d))
			throw new BadSyntaxException();
		nextToken();
	}

	/**
	 * Throws an exception if the current token is not an integer. Otherwise,
	 * returns that integer and moves to the next token.
	 * 
	 * @return the integer value of the current token
	 */
	public int eatIntConstant() {
		if (!matchIntConstant())
			throw new BadSyntaxException();
		int i = (int) tok.nval;
		nextToken();
		return i;
	}

	/**
	 * Throws an exception if the current token is not a string. Otherwise, returns
	 * that string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatStringConstant() {
		if (!matchStringConstant())
			throw new BadSyntaxException();
		String s = tok.sval; // constants are not converted to lower case
		nextToken();
		return s;
	}

	/**
	 * Throws an exception if the current token is not the specified keyword.
	 * Otherwise, moves to the next token.
	 * 
	 * @param w the keyword string
	 */
	public void eatKeyword(String w) {
		if (!matchKeyword(w))
			throw new BadSyntaxException();
		nextToken();
	}

	/**
	 * Throws an exception if the current token is not in the specified operator
	 * list. Otherwise, moves to the next token.
	 * 
	 * @return s the operator string
	 */
	public String eatOpr() {
		if (!matchComparator())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	/**
	 * Throws an exception if the current token is not an identifier. Otherwise,
	 * returns the identifier string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatId() {
		if (!matchId())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	/**
	 * Throws an exception if the current token is not in the specified index list.
	 * Otherwise, moves to the next token.
	 * 
	 * @return s the index string
	 */
	public String eatIndex() {
		if (!matchIndex())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	/**
	 * Throws an exception if the current token is not in the specified aggregate list.
	 * Otherwise, moves to the next token.
	 * 
	 * @return s the aggregate string
	 */
	public String eatAggregate() {
		if (!matchAggregate())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	
	/**
	 * Move to the next token
	 */
	private void nextToken() {
		try {
			tok.nextToken();
		} catch (IOException e) {
			throw new BadSyntaxException();
		}
	}

	/**
	 * Store the specified list of keywords
	 */
	private void initKeywords() {
		keywords = Arrays.asList("select", "from", "where", "and", "insert", "into", "values", "delete", "update",
				"set", "create", "table", "int", "varchar", "view", "as", "index", "on", "using", "order", "by", "asc",
				"desc", "distinct");
	}

	/**
	 * Store the specified list of comparators
	 */
	private void initComparators() {
		comparators = Arrays.asList("=", "<", "<=", ">", ">=", "!=", "<>");
	}

	/**
	 * Store the specified list of indexes type
	 */
	private void initIndex() {
		indexes = Arrays.asList("hash", "btree");
	}

	/**
	 * Store the specified list of aggregate functions
	 */
	private void initAggregates() {
		aggregates = Arrays.asList("min", "max", "sum", "count", "avg");
	}
}