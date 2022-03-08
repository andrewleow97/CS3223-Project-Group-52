package simpleclient;
import java.sql.*;

import java.util.ArrayList;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.network.NetworkDriver;
// path to connect jdbc:simpledb:studentdb
public class SimpleIJTest {
   public static void main(String[] args) {

	  ArrayList<String> queries = new ArrayList<>();
	  
//	  queries.add("select");
	  queries.add("select majorid, sname from student where majorid = 10");
	  queries.add("select dname, cid from dept, course where did = deptid");
	  queries.add("select count (sid) from student");
	  queries.add("select deptid, min (cid) from course group by deptid");
	  queries.add("select distinct GradYear, majorid from student order by gradyear, majorid");
	  queries.add("select GradYear, majorid from student order by gradyear, majorid");
	  queries.add("exit");
	  
      Driver d = new EmbeddedDriver();

      try (Connection conn = d.connect("studentdb", null);
           Statement stmt = conn.createStatement()) {
         System.out.print("\nSQL> ");
         for (String query: queries) {
            // process one line of input
            String cmd = query;
            System.out.println("Query > " + cmd);
            if (query.startsWith("exit"))
               break;
            else if (cmd.startsWith("select"))
               doQuery(stmt, cmd);
            else
               doUpdate(stmt, cmd);
            System.out.print("\nSQL> ");
         }
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
   }

   private static void doQuery(Statement stmt, String cmd) {
      try (ResultSet rs = stmt.executeQuery(cmd)) {
         ResultSetMetaData md = rs.getMetaData();
         int numcols = md.getColumnCount();
         int totalwidth = 0;

         // print header
         for(int i=1; i<=numcols; i++) {
            String fldname = md.getColumnName(i);
            int width = md.getColumnDisplaySize(i);
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for(int i=0; i<totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while(rs.next()) {
            for (int i=1; i<=numcols; i++) {
               String fldname = md.getColumnName(i);
               int fldtype = md.getColumnType(i);
               String fmt = "%" + md.getColumnDisplaySize(i);
               if (fldtype == Types.INTEGER) {
                  int ival = rs.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               }
               else {
                  String sval = rs.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   private static void doUpdate(Statement stmt, String cmd) {
      try {
         int howmany = stmt.executeUpdate(cmd);
         System.out.println(howmany + " records processed");
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
}