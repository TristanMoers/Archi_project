package javaclient;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class JavaClient extends Thread {

    private static Random rnd=new Random();
    


    // Request List keep all request for each client
    private static List<Request> requestsList;
    private static List<Long>[] timeClient;
    private static final String FOLDER = "/Users/tristanmoers/UCL2018-2019/Archi/projet/requests_clients/";
    
    private int nbClient;
    private Request request;
    
    public JavaClient(int nbClient, Request request) {
    	this.nbClient = nbClient;
    	this.request = request;
    }
    
    
    // ============================ POISSON DISTRIBUTION ===============================
    
    // Note L == 1 / lambda
    public double poissonRandomInterarrivalDelay(double L) {
        return Math.log(1.0-Math.random())/-L;
    }
    
    
    
 // =======================================THREAD RUN ==================================
    
    // Each request in stack is poped and by type of request we can execute her correctly
    @Override
    public void run() {
    	// Note -- lambda is 5 seconds, convert to milleseconds
    	long interval= (long)poissonRandomInterarrivalDelay(5.0*1000.0);
        try {
            Thread.sleep(interval);
            
            long startTime = System.currentTimeMillis();
            System.out.println("Client n° : "+this.nbClient+" Requete : "+ this.request.getRequest());          
            //connectionAndExecution(this.request);
            long endTime = System.currentTimeMillis();
			long reqTime = endTime - startTime;
			timeClient[this.nbClient].add(reqTime);
        }
        catch (Exception e) {
        	System.out.println(e);
        }
    }
    
  

    
    // ============================ FILES FUNCTIONS ======================================
    
    private static int getNumberFile(String dirPath) {
        int count = 0;
    	File f = new File(dirPath);
        File[] files = f.listFiles();
        if (files != null)
        for (int i = 0; i < files.length; i++) {
            count++;
        }
        return count;
    }
    
    // Add to stack requests list all requests 
    public static void generateRequest(Integer i) {
    	String FILENAME = FOLDER.concat("requests_c").concat(i.toString()).concat(".txt");
    	String type = null;
    	try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) {

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				switch(sCurrentLine) {
				case "select" 	: type = "select";
								break;
				case "select random" : type = "select random";
										break;
				case "insert" : type = "insert";
								break;
				default : requestsList.add(new Request(type, sCurrentLine));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    

    
    // ================================	SQL FUNCTIONS ======================================
    
    
    public static void connectionAndExecution(Request request)  {
    	try {
            Class.forName("org.mariadb.jdbc.Driver");         
            try(Connection con = DriverManager.getConnection(
                    "jdbc:mysql://192.168.43.135", "Tristan", "archi")) {               
            	// Some examples:
                //testGetAverage(con,Math.abs(rnd.nextInt() % 2000000),Math.abs(rnd.nextInt() % 1000));
                //testSelect(con,Math.abs(rnd.nextInt() % 2000000),1000000);
                //testWrite(con);
            	switch(request.getType()) {
				case "select" 	: select(con, request.getRequest());
								break;
				case "select random" : selectRandom(con, request.getRequest());
										break;
				case "insert" : insert(con, request.getRequest());
								break;
				default : break;
            	}
            }  
            catch (Exception e) {
            	System.out.println(e);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    
    }
    
    
    public static void select(Connection con, String request) throws SQLException {
    	PreparedStatement stmt = con.prepareStatement(request);
    	ResultSet rs = stmt.executeQuery();
    	ResultSetMetaData rsmd = rs.getMetaData();
    	int columnsNumber = rsmd.getColumnCount();
    	 while (rs.next()) {
    		 for(int i = 1; i < columnsNumber; i++)
    		        System.out.print(rs.getString(i) + " ");
    		    System.out.println();
         }
    }
    
    public static void selectRandom(Connection con, String request) throws SQLException {
    	PreparedStatement stmt = con.prepareStatement(request);
    	stmt.setInt(1, Math.abs(rnd.nextInt() % 2000000));
        stmt.setInt(2, 1000000);     
    	ResultSet rs = stmt.executeQuery();
    	ResultSetMetaData rsmd = rs.getMetaData();
    	int columnsNumber = rsmd.getColumnCount();
    	 while (rs.next()) {
    		 for(int i = 1; i < columnsNumber; i++)
    		        System.out.print(rs.getString(i) + " ");
    		    System.out.println();
         }
    }
    
    
    public static void insert(Connection con, String request) throws SQLException {
    	PreparedStatement stmt = con.prepareStatement(request);
        stmt.executeUpdate();         
    }
    
    
    
    
    // ================================== MAIN  ==============================================
    
    
    public static void main(String[] args) {
    	
    	int nbc = getNumberFile(FOLDER);
    	int nbr = 0;
    	timeClient = new List[nbc];
    	Thread[][] clients = new JavaClient[nbc][];
    	for(int i = 0; i < nbc; i++) {
    		requestsList = new ArrayList<Request>();
    		generateRequest(i);
    		nbr = requestsList.size();
    		clients[i] = new JavaClient[nbr];
    		timeClient[i] = new ArrayList<Long>();
    		for(int j = 0; j <nbr ; j++) {
    			clients[i][j] = new JavaClient(i, requestsList.get(j));
    			clients[i][j].run();
    		}
    	}
    	
    	for(int i = 0; i < nbc; i++) {
    		System.out.println("=========== Client n°"+i+" ============");
    		int total = 0;
    		for(int j =0; j < timeClient[i].size(); j++) {
    			total += timeClient[i].get(j);
    			System.out.println("Time Request "+j+" : "+timeClient[i].get(j)+" ms");
    		}
    		System.out.println("TOTAL = "+total+" ms");
    		System.out.println();
    	}
    	
        

    }
    
    
    
    
    
    
    
   /* 
    
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
    */
    
    
    
    
}