package simpleclient.embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CreateStudentDB {
   public static void main(String[] args) {
      Driver d = new EmbeddedDriver();
      String url = "jdbc:simpledb:studentdb";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");
         
         //Creating an index on majorid using btree - create before insertion of records
         s = "create index MajorId on student (MajorId) using btree";
         stmt.executeUpdate(s);
         System.out.println("Index indexMajorId_Student on student created.");

       //Creating an index on SID using hash
         s = "create index SID on student (SId) using hash";
         stmt.executeUpdate(s);
         System.out.println("Index indexSID_Student on student created.");
         
         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {"(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
         	   "(9, 'lee', 10, 2021)",
         	   "(10, 'tom', 40, 2020)",
         	   "(11, 'tay', 50, 2021)",
         	   "(12, 'pua', 60, 2022)",
         	   "(13, 'koh', 70, 2019)",
         	   "(14, 'lim', 80, 2020)",
         	   "(15, 'leow', 90, 2021)",
         	   "(16, 'neo', 100, 2022)",
         	   "(17, 'seow', 110, 2021)",
         	   "(18, 'john', 120, 2019)",
         	   "(19, 'odin', 130, 2021)",
         	   "(20, 'eben', 140, 2019)",
	           "(21, 'xavier', 150, 2022)",
	           "(22, 'miller', 150, 2019)",
	           "(23, 'ron', 160, 2021)",
	           "(24, 'don', 170, 2019)",
	           "(25, 'amir', 170, 2020)",
	           "(26, 'luke', 170, 2019)",
	           "(27, 'harry', 170, 2022)",
	           "(28, 'mat', 170, 2021)",
	           "(29, 'pap', 150, 2019)",
	           "(30, 'hat', 40, 2021)",
	           "(31, 'kile', 50, 2019)",
	           "(32, 'lil', 60, 2021)",
	           "(33, 'pro', 180, 2019)",
	           "(34, 'wite', 190, 2021)",
	           "(35, 'grill', 190, 2019)",
	           "(37, 'toh', 200, 2020)",
	           "(38, 'kong', 200, 2019)",
	           "(39, 'sim', 210, 2019)",
	           "(40, 'goh', 210, 2022)",
	           "(41, 'hip', 220, 2019)",
	           "(42, 'ban', 230, 2020)",
	           "(43, 'pan', 230, 2022)",
	           "(44, 'tim', 230, 2019)",
	           "(45, 'cook', 240, 2021)",
	           "(46, 'chef', 240, 2022)",
	           "(47, 'boss', 250, 2020)",
	           "(48, 'worker', 250, 2019)",
	           "(49, 'life', 260, 2020)",
	           "(50, 'quite', 260, 2022)",
	           "(51, 'apoll', 270, 2019)",
	           "(52, 'jet', 280, 2022)"};
	         
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(8))";
         stmt.executeUpdate(s);
         System.out.println("Table DEPT created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {"(10, 'compsci')",
                 "(20, 'compeng')",
                 "(30, 'compsec')",
                 "(40, 'infosec')",
                 "(50, 'infotech')",
                 "(60, 'psych')",
                 "(70, 'hist')",
                 "(80, 'geog')",
                 "(90, 'lit')",
                 "(100, 'agri')",
                 "(110, 'vet')",
                 "(120, 'horti')",
                 "(130, 'plantsci')",
                 "(140, 'astro')",
                 "(150, 'chem')",
                 "(160, 'foodsci')",
                 "(170, 'lifesci')",
                 "(180, 'physicalgeog')",
                 "(190, 'bio')",
                 "(200, 'earthsci')",
                 "(210, 'forensci')",
                 "(220, 'materialsci')",
                 "(230, 'physics')",
                 "(240, 'biomedsci')",
                 "(250, 'environsci')",
                 "(260, 'gensci')",
                 "(270, 'math')",
                 "(280, 'sportssci')",
                 "(290, 'archi')",
                 "(300, 'maintenance')",
                 "(310, 'survey')",
                 "(320, 'planning')",
                 "(330, 'construction')",
                 "(340, 'propertymgmt')",
                 "(350, 'accounting')",
                 "(360, 'entrepreneurship')",
                 "(370, 'mgmt')",
                 "(380, 'qualitymgmt')",
                 "(390, 'busines')",
                 "(400, 'finance')",
                 "(410, 'marketing')",
                 "(420, 'retail')",
                 "(430, 'HRmgmt')",
                 "(440, 'interiordesign')",
                 "(450, 'theatre')",
                 "(460, 'music')",
                 "(470, 'dance')",
                 "(480, 'industrialdesign')",
                 "(490, 'fashion')",
                 "(500, 'earlychild')",
                 "(510, 'pedagogy')",
                 "(520, 'aeroeng')",
                 "(530, 'drama')"};
         
         for (int i=0; i<deptvals.length; i++)
            stmt.executeUpdate(s + deptvals[i]);
         System.out.println("DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         stmt.executeUpdate(s);
         System.out.println("Table COURSE created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {"(12, 'db systems', 10)",
                                "(22, 'compilers', 10)",
                                "(32, 'calculus', 270)",
                                "(42, 'algebra', 270)",
                                "(52, 'acting', 530)",
                                "(62, 'elocution', 530)",
                                "(72, 'security', 10)",
                                "(82, 'statistic', 270)",
                                "(92, 'screenwriting', 530)",
        		 				"(102, 'machine learning', 10)",
        		 				"(112, 'information systems', 10)",
        		 				"(122, 'big data', 10)",
        		 				"(132, 'data structures and algorithms', 10)",
        		 				"(142, 'computer org', 10)",
        		 				"(152, 'software engineer', 10)",
        		 				"(162, 'operating systems', 10)",
        		 				"(172, 'media computing', 10)",
        		 				"(182, 'cloud computing', 10)",
        		 				"(192, 'computer networks', 10)",
        		 				"(202, 'parallel computing', 10)",
        		 				"(212, 'theory computation', 10)",
        		 				"(222, 'competitive programming', 10)",
        		 				"(232, 'game dev', 10)",
        		 				"(242, 'sofware testing', 10)",
        		 				"(252, 'game dev', 10)",
        		 				"(262, 'wireless network', 10)",
        		 				"(272, 'distributed db', 10)",
        		 				"(282, 'sound and music comp', 10)",
        		 				"(292, 'web security', 10)",
        		 				"(302, 'number theory', 270)",
        		 				"(312, 'linear algebra', 270)",
        		 				"(322, 'trigonometry', 270)",
        		 				"(332, 'precalculas', 270)",
        		 				"(342, 'derivatives', 270)",
        		 				"(352, 'determinants', 270)",
        		 				"(362, 'differential equations', 270)",
        		 				"(372, 'integration', 270)",
        		 				"(382, 'area', 270)",
        		 				"(392, 'volume', 270)",
        		 				"(402, 'multiplication', 270)",
        		 				"(412, 'division', 270)",
        		 				"(422, 'addition', 270)",
        		 				"(432, 'subtraction', 270)",
        		 				"(442, 'competitive math', 270)",
        		 				"(452, 'signal and system', 270)",
        		 				"(462, 'engineering math I', 270)",
        		 				"(472, 'engineering math II', 270)",
        		 				"(482, 'place value', 270)",
        		 				"(492, 'rounding off', 270)",
        		 				"(502, 'number line', 270)",
        		 				"(512, 'fractions', 270)",
        		 				"(522, 'film', 530)",
        		 				"(532, 'theater', 530)",
        		 				"(542, 'directing', 530)"};
         
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {"(13, 12, 'turing', 2018)",
                 "(23, 12, 'turing', 2018)",
                 "(33, 22, 'newton', 2018)",
                 "(43, 22, 'newton', 2018)",
                 "(53, 32, 'turing', 2018)",
                 "(63, 32, 'newton', 2018)",
                 "(73, 32, 'lisa', 2018)",
                 "(83, 42, 'lisa', 2018)",
                 "(93, 42, 'jennie', 2018)",
                 "(103, 52, 'jennie', 2018)",
                 "(113, 52, 'jennie', 2018)",
                 "(123, 62, 'jackson', 2019)",
                 "(133, 62, 'jackson', 2019)",
                 "(143, 72, 'turing', 2019)",
                 "(153, 72, 'newton', 2019)",
                 "(163, 82, 'einstein', 2019)",
                 "(173, 82, 'jennie', 2019)",
                 "(183, 82, 'newton', 2019)",
                 "(193, 92, 'einstein', 2019)",
                 "(203, 92, 'lisa', 2019)",
                 "(213, 102, 'newton', 2019)",
                 "(223, 112, 'yuna', 2020)",
                 "(233, 122, 'yuna', 2020)",
                 "(243, 132, 'newton', 2020)",
                 "(253, 142, 'einstein', 2020)",
                 "(263, 142, 'turing', 2020)",
                 "(273, 152, 'newton', 2020)",
                 "(283, 162, 'einstein', 2020)",
                 "(293, 172, 'turing', 2020)",
                 "(303, 182, 'newton', 2020)",
                 "(313, 182, 'einstein', 2020)",
                 "(323, 192, 'shakespeare', 2021)",
                 "(333, 202, 'newton', 2021)",
                 "(343, 212, 'einstein', 2021)",
                 "(353, 222, 'turing', 2021)",
                 "(363, 232, 'newton', 2021)",
                 "(373, 242, 'einstein', 2021)",
                 "(383, 252, 'turing', 2021)",
                 "(393, 262, 'newton', 2021)",
                 "(403, 262, 'einstein', 2021)",
                 "(413, 272, 'turing', 2021)",
                 "(423, 282, 'newton', 2022)",
                 "(433, 282, 'einstein', 2022)",
                 "(443, 292, 'turing', 2022)",
                 "(453, 292, 'newton', 2022)",
                 "(463, 302, 'einstein', 2022)",
                 "(473, 312, 'turing', 2022)",
                 "(483, 322, 'newton', 2022)",
                 "(493, 332, 'einstein', 2022)",
                 "(503, 342, 'turing', 2022)",
                 "(513, 342, 'newton', 2022)",
                 "(523, 352, 'einstein', 2022)",
                 "(533, 362, 'brando', 2022)"};
         
         for (int i=0; i<sectvals.length; i++)
            stmt.executeUpdate(s + sectvals[i]);
         System.out.println("SECTION records inserted.");
         
         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         stmt.executeUpdate(s);
         System.out.println("Table ENROLL created.");

         //Creating an index on studentid using hash
         s = "create index StudentId on enroll (StudentId) using hash";
         stmt.executeUpdate(s);
         System.out.println("Index indexStudentId_Enroll on enroll created.");
         
         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {"(14, 1, 13, 'A')",
                                "(24, 1, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(44, 4, 33, 'B' )",
                                "(54, 4, 53, 'A' )",
                                "(64, 6, 53, 'A' )",
                                "(74, 7, 63, 'A')",
                                "(84, 8, 73, 'A')",
                                "(94, 9, 83, 'B+')",
                                "(104, 9, 73, 'A')",
                                "(114, 10, 93, 'B')",
                                "(124, 10, 103, 'A')",
                                "(134, 11, 93, 'B')",
                                "(144, 11, 103, 'A')",
                                "(154, 12, 113, 'A')",
                                "(164, 12, 123, 'C')",
                                "(174, 13, 113, 'B+')",
                                "(184, 13, 123, 'B')",
                                "(194, 14, 133, 'A')",
                                "(204, 14, 143, 'B+')",
                                "(214, 15, 133, 'A')",
                                "(224, 15, 143, 'B+')",
                                "(234, 16, 153, 'A')",
                                "(244, 16, 163, 'B')",
                                "(254, 17, 153, 'A')",
                                "(264, 17, 163, 'B+')",
                                "(274, 18, 173, 'A')",
                                "(284, 18, 183, 'A')",
                                "(294, 19, 173, 'C')",
                                "(304, 19, 183, 'A')",
                                "(314, 20, 193, 'B')",
                                "(324, 20, 203, 'B')",
                                "(334, 21, 193, 'A')",
                                "(344, 21, 203, 'A')",
                                "(354, 22, 213, 'B+')",
                                "(364, 22, 223, 'A')",
                                "(374, 23, 213, 'B')",
                                "(384, 23, 223, 'A')",
                                "(394, 24, 213, 'A')",
                                "(404, 24, 223, 'B+')",
                                "(414, 25, 233, 'A')",
                                "(424, 25, 243, 'B')",
                                "(434, 26, 233, 'A')",
                                "(444, 26, 243, 'B+')",
                                "(454, 27, 253, 'A')",
                                "(464, 27, 263, 'B+')",
                                "(474, 28, 253, 'A')",
                                "(484, 28, 273, 'A')",
                                "(494, 29, 283, 'C')",
                                "(504, 29, 273, 'A')",
                                "(514, 30, 283, 'B+')",
                                "(524, 30, 293, 'A')",
                                "(534, 31, 303, 'B+')",
                                "(544, 31, 293, 'B+')",
                                "(554, 32, 303, 'A')"};
         
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");
         

      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
