package javaclient;

import java.awt.List;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class JavaClient extends Thread {

    private static Random rnd=new Random();
    
    /////////////////////////////////////////////////////////////////////
    
    private int nbClient;
    private int nbRequest;
    private HashMap<String, String[]> requests;
    
    
    public JavaClient(int nbClient,int nbRequest, HashMap<String, String[]> requests) {
    	this.nbClient = nbClient;
    	this.nbRequest = nbRequest;
    	this.requests = requests;
    }
    
    
    
    // Note L == 1 / lambda
    public double poissonRandomInterarrivalDelay(double L) {
        return Math.log(1.0-Math.random())/-L;
    }
    
    
    public void run() {
    	// Note -- lambda is 5 seconds, convert to milleseconds
    	long interval= (long)poissonRandomInterarrivalDelay(5.0*1000.0);
        try {
            Thread.sleep(interval);
        }
        catch (Exception e) {
        	System.out.println(e);
        }
    }
    
    

    
    /////////////////////////////////////////////////////////////////////
    
    public static void main(String[] args) {
    	int nb = 2;
    	Thread[] clients = new JavaClient[nb];
    	for(int i = 0; i < nb; i++) {
		   clients[i].start();
    	}
    	
        try {
            // Load the JDBC driver, so your Java program can talk with your database.
            // You have to download the driver (called Connector/J) from
            //    https://mariadb.com/kb/en/library/about-mariadb-connector-j/
            // and add the jar file to your project, otherwise you will get a
            // ClassNotFound expection.
            Class.forName("org.mariadb.jdbc.Driver");
            
            // Connect to the database. Very similar to using the mysql commandline tool.
            // Of course, you have to change the IP address and username and password.
            try(Connection con = DriverManager.getConnection(
                    "jdbc:mysql://192.168.43.135", "Tristan", "archi")) {
                
            	// Some examples:
                //testGetAverage(con,Math.abs(rnd.nextInt() % 2000000),Math.abs(rnd.nextInt() % 1000));
                testSelect(con,Math.abs(rnd.nextInt() % 2000000),1000000);
                //testWrite(con);
            }           
        }
        catch (Exception e) {
            System.out.println(e);
        }

    }
    
    // Example query where most of the processing happens on the server.
    private static void testGetAverage(Connection con,int startRow,int numberOfRows) throws SQLException {
        // Send the query to the database.
        PreparedStatement stmt = con.prepareStatement("SELECT AVG(t.salary) FROM (SELECT salary FROM employees.salaries LIMIT ? OFFSET ?) as t");
        stmt.setInt(1, numberOfRows);
        stmt.setInt(2, startRow);        
        ResultSet rs = stmt.executeQuery();
        
        // Get the result of the query. The while-loop is not really needed
        // here because our example query will only return one result line
        while (rs.next()) {
            double averageSalary=rs.getDouble(1);  // get column 1 of the result          
        }
    }
    
    // Example query that generates some network traffic because data is sent from the server
    // to the client.
    private static void testSelect(Connection con,int startRow,int numberOfRows) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("SELECT * FROM employees.salaries LIMIT ? OFFSET ?");
        stmt.setInt(1, numberOfRows);
        stmt.setInt(2, startRow);        
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            // we are not really doing anything with the data...
            int employeeNumber=rs.getInt(1);
            int salary=rs.getInt(2);
            Date from=rs.getDate(3);
            Date to=rs.getDate(4);  
        }
    }
    
    // Example query that writes to the database.
    private static void testWrite(Connection con) throws SQLException {
        // random employee
        int employeeNumber=10001+(Math.abs(rnd.nextInt()) % 100000);
        // random dates
        long r=Math.abs(rnd.nextLong()) % 100000000000000L;
        Date from=new java.sql.Date(r);
        Date to=new java.sql.Date(r+10000000);
        
        try {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO employees.salaries VALUE (?,123,?,?)");
            stmt.setInt(1,employeeNumber);
            stmt.setDate(2,from);
            stmt.setDate(3,to);
            stmt.executeUpdate();         
        }
        catch(SQLIntegrityConstraintViolationException e) {
            // The salaries table uses the employee number and the from-date as primary key.
            // Since we are generating random employee numbers and random dates, there is a certain
            // probablity that the row already exists. Let's just ignore errors for this test.
        }
        
        // Clean up your database afterwards with
        //   DELETE from employees.salaries WHERE salary=123
    }
    
    
    
    
    
}