package simpleclient;
import java.sql.*;

import java.util.ArrayList;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.network.NetworkDriver;

/**
 * The test class to test various query types and combinations for the SimpleDB+ DBMS system.
 * This class will be used during the demo to show each function 
 * we are supposed to complete as part of project requirements.
 * 
 *
 */
public class SimpleIJTest {
   public static void main(String[] args) {

	  ArrayList<String> queries = new ArrayList<>();
	  
//	  queries.add("select");
//	  //Test operators
//	  queries.add("select majorid from student");
//	  queries.add("select majorid from student where majorid = 10");
//	  queries.add("select majorid from student where majorid < 30");
//	  queries.add("select majorid from student where majorid <= 30");
//	  queries.add("select majorid from student where majorid > 30");
//	  queries.add("select majorid from student where majorid >= 30");
//	  queries.add("select majorid from student where majorid <> 30");
//	  queries.add("select majorid from student where majorid != 30");
//	  
	  //Test index creation
//	  queries.add("create index DId on dept (did) using btree");
//	  queries.add("create index CId on course (cid) using hash");
	  
	  //Test index select
//	  queries.add("select sid from student where sid = 5");
//	  queries.add("select sid from student where sid < 5");
//	  queries.add("select sid from student where sid > 5");
//	  queries.add("select sid from student where sid != 5");
//	  queries.add("select sid from student where sid <> 5");
//	  queries.add("select sid from student where sid >= 5");
//	  queries.add("select sid from student where sid <= 5");
//	  
//	  //Test aggregate
//	  queries.add("select sid from student");
//	  queries.add("select count (sid) from student");
//	  queries.add("select max (sid) from student");
//	  queries.add("select min (sid) from student");
//	  queries.add("select sum (sid) from student");
//	  queries.add("select avg (sid) from student");
//	  queries.add("select avg (majorid) from student");
//	  queries.add("select count (majorid) from student");
//	  
//	  //Test distinct
//	  queries.add("select distinct deptid from course");
//	  queries.add("select distinct GradYear, majorid from student order by gradyear, majorid");
//	  
//	  //Test sorting (order by)
//	  queries.add("select eid from enroll order by eid");
//	  queries.add("select eid from enroll order by eid asc");
//	  queries.add("select eid from enroll order by eid desc");
//	  queries.add("select eid, studentid from enroll order by eid desc, studentid asc");
//	  queries.add("select majorid, sid from student order by majorid asc, sid desc");
//	  
//	  //Test group by
//	  queries.add("select count (sname), gradyear from student group by gradyear");
//	  queries.add("select count (sname), gradyear from student group by gradyear order by gradyear desc");
//	  queries.add("select deptid, min (cid) from course group by deptid");
//	  queries.add("select deptid, avg (cid) from course group by deptid");
//	  
//	  //2 table queries
	  //Equality
//	  queries.add("select dname, cid from dept, course where did = deptid");
//	  queries.add("select sname, sid from student, enroll where sid = studentid order by sname asc");
//	  queries.add("select sname, count (sid) from student, enroll where sid = studentid group by sname");
//	  queries.add("select sname, sid, majorid from student, enroll where sid = studentid and majorid > 10");//error cause select pred on idnex
//	  queries.add("select distinct sid, majorid from student, enroll where sid = studentid");
//	  queries.add("select sname, sid, majorid, gradyear from student, enroll where sid = studentid and gradyear > 2020");
//	  
//	  //Non equi
//	  queries.add("select did, deptid, dname, cid from dept, course where did < deptid");
//	  queries.add("select did, deptid, dname, cid from dept, course where did <= deptid");
//	  queries.add("select did, deptid, dname, cid from dept, course where did > deptid");
//	  queries.add("select did, deptid, dname, cid from dept, course where did <= deptid");
//	  queries.add("select did, deptid, dname, cid from dept, course where did <> deptid");
//	  queries.add("select did, deptid, dname, cid from dept, course where did != deptid");
//	  
//	  //4 table queries
//	  //Equality
//	  queries.add("select sid, dname from student, dept, course, enroll where majorid = did and did = deptid and sid = studentid order by sid desc");
//	  //Non equi
//	  queries.add("select sid, majorid, did, deptid, prof from student, dept, course, section where majorid <> did and did = deptid and cid = courseid");
//	  queries.add("select sid, majorid, did, deptid, prof from student, dept, course, section where majorid <> did and did = deptid and cid < courseid");

	  
	  queries.add("exit");
	  
      Driver d = new EmbeddedDriver();

      // start connection to studentdb database
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

   /**
    * Perform query operation 
    * @param stmt statement object to hold the SQL statement
    * @param cmd SQL command to be executed
    */
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