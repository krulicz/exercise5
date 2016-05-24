package wdsr.exercise5;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Main {

	final static String DB_URL = "hsqldb";
	final static String DB_SUBSUBPROTOCOL = "mem";
	final static String DB_NAME = "test-db";
	//static Logger log = LogManager.getLogger(Main.class.getName());

	public static Connection getConnection() throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "SA");
		connectionProps.put("password", "");
		conn = DriverManager.getConnection("jdbc:" + DB_URL + ":" + DB_SUBSUBPROTOCOL + ":" + DB_NAME + ";create=true",
				connectionProps);
		return conn;
	}

	public static void querySQL() throws SQLException {
		String[] createTable = {
				"create table student(pkey INTEGER CONSTRAINT student_PK PRIMARY KEY,name VARCHAR(100) NOT NULL,sex VARCHAR(6) NOT NULL CONSTRAINT student_SEX_CK CHECK (sex='male' OR sex='female'),age INTEGER NOT NULL CONSTRAINT student_AGE_CK CHECK (age>0 AND age<130),level INTEGER NOT NULL CONSTRAINT student_level_CK CHECK (level>0));",
				"create table faculty(pkey INTEGER CONSTRAINT faculty_PK PRIMARY KEY,name VARCHAR(100) NOT NULL);",
				"create table class(pkey INTEGER CONSTRAINT class_PK PRIMARY KEY,name VARCHAR(100) NOT NULL,pkey_faculty INTEGER NOT NULL CONSTRAINT class_faculty_FKEY REFERENCES faculty(pkey));",
				"create table enrollment(pkey_student INTEGER CONSTRAINT enrollment_1_PK REFERENCES student(pkey), pkey_class INTEGER CONSTRAINT enrollment_2_PK REFERENCES class(pkey), CONSTRAINT enrollment_pkey PRIMARY KEY (pkey_student,pkey_class));" };

		String[] insertTable = { "INSERT INTO student VALUES(1,'John Smith','male',23,2);",
				"INSERT INTO student VALUES(2,'Rebecca Milson','female',27,3);",
				"INSERT INTO student VALUES(3,'George Heartbreaker','male',19,1);",
				"INSERT INTO student VALUES(4,'Deepika Chopra','female',25,3);",
				"INSERT INTO faculty VALUES(100,'Engineering');", 
				"INSERT INTO faculty VALUES(101,'Philosophy');",
				"INSERT INTO faculty VALUES(102,'Law and administration');",
				"INSERT INTO faculty VALUES(103,'Languages');",
				"INSERT INTO class VALUES(1000,'Introduction to labour law',102);",
				"INSERT INTO class VALUES(1001,'Graph algorithms',100);",
				"INSERT INTO class VALUES(1002,'Existentialism in 20th century',101);",
				"INSERT INTO class VALUES(1003,'English Grammar',103);",
				"INSERT INTO class VALUES(1004,'From Plato to Kant',101);", "INSERT INTO enrollment VALUES(1,1000);",
				"INSERT INTO enrollment VALUES(1,1002);", "INSERT INTO enrollment VALUES(1,1003);",
				"INSERT INTO enrollment VALUES(1,1004);", "INSERT INTO enrollment VALUES(2,1002);",
				"INSERT INTO enrollment VALUES(2,1003);", "INSERT INTO enrollment VALUES(4,1000);",
				"INSERT INTO enrollment VALUES(4,1002);", "INSERT INTO enrollment VALUES(4,1003);" };
		String[] selectTable = { 
				"select pkey,name from student;",
				"select	pkey, s.name as name from Student s full join enrollment e on(s.pkey = e.pkey_student) WHERE e.pkey_student IS NULL AND e.pkey_class IS NULL;",
				"select pkey,name from student s join enrollment e on(s.pkey=e.pkey_student) join class c on (c.pkey=e.pkey_class) WHERE c.name='Existentialism in 20th century' AND s.sex='female';",
				"select name as f.name from faculty f join class c on(f.pkey=c.pkey_faculty) full join enrollment e on(c.pkey=e.pkey_class) WHERE e.pkey_student is null;", 
				"select max(s.age) as age from class c full join enrollment e on(c.pkey=e.pkey_class) join student s on(s.pkey=e.pkey_student) WHERE c.name='Introduction to labour law' order by age;", 
				"select c.name as name from class c WHERE ((Select count(*) from enrollment e join class c1 on(c1.pkey=e.pkey_class) WHERE c1.pkey=c.pkey group by c1.pkey) >1); ",
				"select level,avg(age) as age from student group by level order by level;" };

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;

		try {
			dbConnection = getConnection();
			for (int i = 0; i < createTable.length; i++) {
				preparedStatement = dbConnection.prepareStatement(createTable[i]);
				// execute select SQL statement
				preparedStatement.executeUpdate();
				System.out.println("Create table number: " + (i + 1) + " done.");
			}
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			for (int i = 0; i < insertTable.length; i++) {
				preparedStatement = dbConnection.prepareStatement(insertTable[i]);
				preparedStatement.executeUpdate();
				System.out.println("Insert query number: " + (i + 1) + " done.");
			}
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			for (int i = 0; i < selectTable.length; i++) {
				preparedStatement = dbConnection.prepareStatement(selectTable[i]);
				ResultSet rs = preparedStatement.executeQuery();
				System.out.println("Wynik zapytania " + (i + 1) + ":");
				while (rs.next()) {
					if (i == 0) {
						String a = rs.getString("pkey");
						String b = rs.getString("name");
						System.out.println("pkey=" + a + ",name=" + b);
					}
					if (i == 1) {
						String a = rs.getString("pkey");
						String b = rs.getString("name");
						System.out.println("pkey=" + a + ",name=" + b);
					}
					if (i == 2) {
						String a = rs.getString("pkey");
						String b = rs.getString("name");
						System.out.println("pkey=" + a + ",name=" + b);
					}
					if (i == 3) {
						String b = rs.getString("name");
						System.out.println("name=" + b);
					}
					if (i == 4) {
						Integer a = rs.getInt("age");
						System.out.println("age=" + a);
					}
					if (i == 5) {
						String a = rs.getString("name");
						System.out.println("name=" + a);
					}
					if (i == 6) {
						Integer a = rs.getInt("level");
						Float b = rs.getFloat("age");
						System.out.println("level=" + a + ",avg age=" + b);
					}

				}

				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
	}

	public static void main(String[] args) {
		try {

			querySQL();

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		}
	}
}
