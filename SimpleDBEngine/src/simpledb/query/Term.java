package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.*;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
   private Expression lhs, rhs;
   private String opr;
   
   /**
    * Create a new term that compares two expressions
    * for equality.
    * @param lhs  the LHS expression
    * @param rhs  the RHS expression
    * @param opr the operator
    */
   public Term(Expression lhs, Expression rhs, String opr) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.opr = opr;
   }
   
   /**
    * Return true if both of the term's expressions
    * evaluate true w.r.t each other according to the operator,
    * with respect to the specified scan.
    * @param s the scan
    * @return true if both expressions have the corresponding value in the scan
    */
   public boolean isSatisfied(Scan s) { 

      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      switch(this.opr) {
      case "=":
    	  return lhsval.compareTo(rhsval) == 0;
      case "<":
    	  return lhsval.compareTo(rhsval) < 0;
      case "<=":
    	  return lhsval.compareTo(rhsval) < 0 || lhsval.compareTo(rhsval) == 0;
      case ">":
    	  return lhsval.compareTo(rhsval) > 0;
      case ">=":
    	  return lhsval.compareTo(rhsval) > 0 || lhsval.compareTo(rhsval) == 0;
      case "!=":
    	  return lhsval.compareTo(rhsval) != 0;
      case "<>":
    	  return lhsval.compareTo(rhsval) != 0;
	  default:
		  return false;  
    	  
      }
   }
   
   /**
    * Calculate the extent to which selecting on the term reduces 
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   /**
    * Determine if this term is of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }
   
   /**
    * Determine if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }
   
   /**
    * Return true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   /**
    * Convert the term to string form.
    * @return term in string form
    */
   public String toString() {
      return lhs.toString() + opr.toString() + rhs.toString();
   }
   
   /**
    * Return the lhs expression in string form.
    * @return lhs expression in string form
    */
   public String LHS() {
	   return lhs.toString();
   }
   
   /**
    * Return the rhs expression in string form.
    * @return rhs expression in string form
    */
   public String RHS() {
	   return rhs.toString();
   }
   
   /**
    * Return the operator
    * @return operator
    */
   public String operator() {
	   return opr;
   }
   
   
   /**
    * Check if both rhs and lhs are fields.
    * @return true if both are fields else false
    */
   public boolean compareField() {
	   if (lhs.isFieldName() && rhs.isFieldName()) {
		   return true;
	   }
	   return false;
   }
   
}
