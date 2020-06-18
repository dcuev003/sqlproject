/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class Ticketmaster{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public Ticketmaster(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + Ticketmaster.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		Ticketmaster esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new Ticketmaster (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add User");
				System.out.println("2. Add Booking");
				System.out.println("3. Add Movie Showing for an Existing Theater");
				System.out.println("4. Cancel Pending Bookings");
				System.out.println("5. Change Seats Reserved for a Booking");
				System.out.println("6. Remove a Payment");
				System.out.println("7. Clear Cancelled Bookings");
				System.out.println("8. Remove Shows on a Given Date");
				System.out.println("9. List all Theaters in a Cinema Playing a Given Show");
				System.out.println("10. List all Shows that Start at a Given Time and Date");
				System.out.println("11. List Movie Titles Containing \"love\" Released After 2010");
				System.out.println("12. List the First Name, Last Name, and Email of Users with a Pending Booking");
				System.out.println("13. List the Title, Duration, Date, and Time of Shows Playing a Given Movie at a Given Cinema During a Date Range");
				System.out.println("14. List the Movie Title, Show Date & Start Time, Theater Name, and Cinema Seat Number for all Bookings of a Given User");
				System.out.println("15. EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddUser(esql); break;
					case 2: AddBooking(esql); break;
					case 3: AddMovieShowingToTheater(esql); break;
					case 4: CancelPendingBookings(esql); break;
					case 5: ChangeSeatsForBooking(esql); break;
					case 6: RemovePayment(esql); break;
					case 7: ClearCancelledBookings(esql); break;
					case 8: RemoveShowsOnDate(esql); break;
					case 9: ListTheatersPlayingShow(esql); break;
					case 10: ListShowsStartingOnTimeAndDate(esql); break;
					case 11: ListMovieTitlesContainingLoveReleasedAfter2010(esql); break;
					case 12: ListUsersWithPendingBooking(esql); break;
					case 13: ListMovieAndShowInfoAtCinemaInDateRange(esql); break;
					case 14: ListBookingInfoForUser(esql); break;
					case 15: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddUser(Ticketmaster esql){//1
		
		Scanner scan = new Scanner(System.in);
        String command = "INSERT INTO users(email, lname, fname, phone, pwd) VALUES (";


        System.out.println("Please enter the following values: email, last name, first name, phone number, password");
        command += scan.nextLine();

        command+= ");";

        try {
                esql.executeUpdate(command);
        } catch(Exception e) {
                System.err.println (e.getMessage ());
        }
	}

	
	public static void AddBooking(Ticketmaster esql){//2
		
		Scanner scan = new Scanner(System.in);
        String command = "INSERT INTO Bookings(bid, status, bdatetime, seats, sid, email) VALUES (";

        System.out.println("Please enter the following values: booking id, status, date/time, seats, show ID, email");
        command += scan.nextLine() + ");";                                                            
        try {
        	esql.executeUpdate(command);
        } catch(Exception e) {                                                                                                
        	System.err.println (e.getMessage ());
        }
		
	}
	
	public static void AddMovieShowingToTheater(Ticketmaster esql){//3
		Scanner scan = new Scanner(System.in);
		String command;
		int mvid;
		String title;
		String date;
		String country;
		String description;
		int duration;
		String language;
		String genre;
		
		int sid;
		String sDate;
		String sTime;
		String eTime;
		
		int tid;
		
		System.out.println("Please input the movie ID");
		mvid = scan.nextInt();
		
		System.out.println("Please input the movie title");
		title = scan.nextLine();
		
		System.out.println("Please input the movie release date");
		date = scan.nextLine();
		
		System.out.println("Please input the movie's release country");
		country = scan.nextLine();
		
		System.out.println("Please input the movie description");
		description = scan.nextLine();
		
		System.out.println("Please input the movie duration in seconds");
		duration = scan.nextInt();
		
		System.out.println("Please input the movie language code");
		language = scan.nextLine();
		
		System.out.println("Please input the movie genre");
		genre = scan.nextLine();
		
		System.out.println("Please enter the show ID");
		sid = scan.nextInt();
		
		System.out.println("Please input the show date");
		sDate = scan.nextLine();
		
		System.out.println("Please input the show start time");
		sTime = scan.nextLine();
		
		System.out.println("Please input the show end time");
		eTime = scan.nextLine();
		
		System.out.println("Please input the theater ID");
		tid = scan.nextInt();
		
		command = "INSERT INTO Movies(mvid, title, rdate, country, description, duration, lang, genre) VALUES (";
		command += mvid + ", '" + title + "', '" + date + "', '" + country + "', '" + description + "', " + duration + ", '" + language + "', '" + genre + "');";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
		
		command = "INSERT INTO Shows(sid, mvid, sdate, sttime, edtime) VALUES (";
		command += sid + ", " + mvid + ", '" + sDate + "', '" + sTime + "', '" + eTime + "');";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
		
		command = "INSERT INTO Plays(sid, tid) VALUES (" + sid + ", " + tid + ");";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
	}
	
	public static void CancelPendingBookings(Ticketmaster esql){//4
		String command = "DELETE FROM Bookings WHERE status = 'Pending';";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
		
	}
	
	public static void ChangeSeatsForBooking(Ticketmaster esql) throws Exception{//5
		
	}
	
	public static void RemovePayment(Ticketmaster esql){//6
		Scanner scan = new Scanner(System.in);
        String command = "UPDATE Bookings SET status = 'Cancelled' WHERE bid = ";
        int bid;

        System.out.println("Please enter the booking id:");
        bid = scan.nextInt();

        command += bid;

        try {
                esql.executeUpdate(command);
        } catch(Exception e) {
                System.err.println (e.getMessage ());
        }

        command = "DELETE FROM Payment WHERE bid = " + bid + ";";

        try {
                esql.executeUpdate(command);
        } catch(Exception e) {
                System.err.println (e.getMessage ());
        }
		
	}
	
	public static void ClearCancelledBookings(Ticketmaster esql){//7
		String command = "DELETE FROM Bookings WHERE status = 'Cancelled';";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
    	System.err.println (e.getMessage ());
    	}
	}
	
	public static void RemoveShowsOnDate(Ticketmaster esql){//8
		//DELETE FROM shows WHERE sdate = '1/1/2019'
		String command = "DELETE FROM shows WHERE sdate = '";
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Please input the date");
		command += scan.nextLine() + "';";
		
		try {
            esql.executeUpdate(command);
		} catch(Exception e) {
    	System.err.println (e.getMessage ());
    	}
	}
	
	public static void ListTheatersPlayingShow(Ticketmaster esql){//9
		//
		//SELECT T.tname FROM theaters T, cinemas C, shows S, plays P 
		//WHERE P.sid = 1 AND P.sid = S.sid AND P.tid = T.tid AND T.cid = C.cid;
		
		String command = "SELECT T.tname FROM theaters T, cinemas C, shows S, plays P WHERE P.sid = ";
        Scanner scan = new Scanner(System.in);
        int sid;

        System.out.println("Please enter the show ID");
        sid = scan.nextInt();

        command += sid + " AND P.sid = S.sid AND P.tid = T.tid AND T.cid = C.cid;";

        try {
                esql.executeQueryAndPrintResult(command);
        } catch(Exception e) {
                System.err.println (e.getMessage ());
        }
	}
	
	public static void ListShowsStartingOnTimeAndDate(Ticketmaster esql){//10
		//
		 //SELECT * FROM shows WHERE sdate = '1/1/2009' AND sttime = '9:25';
		
		String command = "SELECT * FROM shows WHERE sdate = '";
        Scanner scan = new Scanner(System.in);
        String user_input;

        System.out.println("Please enter the date in the format dd/mm/yyyy");
        user_input = scan.nextLine();

        command += user_input + "' AND sttime = '";

        System.out.println("Please enter the time in the format hh:mm");
        user_input = scan.nextLine();

        command += user_input + "';";

        System.out.println(command);

        try {
                esql.executeQueryAndPrintResult(command);
        } catch(Exception e) {
                System.err.println (e.getMessage ());
        }
		
	}

	public static void ListMovieTitlesContainingLoveReleasedAfter2010(Ticketmaster esql){//11
		//
		String command = "SELECT title FROM movies WHERE title LIKE '%Love%' AND rdate > '12/31/2010';";
		
		try {
            esql.executeQueryAndPrintResult(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
	}

	public static void ListUsersWithPendingBooking(Ticketmaster esql){//12
		//
		String command = "SELECT U.fname, U.lname, U.email FROM users U, bookings B WHERE B.status = 'Pending' AND B.email = U.email;";
		
		try {
            esql.executeQueryAndPrintResult(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
	}

	public static void ListMovieAndShowInfoAtCinemaInDateRange(Ticketmaster esql){//13
		//
		//SELECT M.title, M.duration, S.sdate, S.sttime FROM movies M, shows S, cinemas C, theaters T, plays P 
		//WHERE M.title = 'Avatar' AND C.cid = '1' AND C.cid = T.cid AND T.tid = P.tid 
		//AND S.sid = P.sid AND M.mvid = S.mvid 
		//AND S.sdate >= '1/1/1997' AND S.sdate <= '1/1/2030';
		Scanner scan = new Scanner(System.in);
		String command = "SELECT M.title, M.duration, S.sdate, S.sttime FROM movies M, shows S, cinemas C, theaters T, plays P WHERE M.title = '";
		int cid;
		
		System.out.println("Please enter the movie title:");
		command += scan.nextLine() + "' AND C.cid = '";
		
		System.out.println("Please enter the cinema ID");
		command += scan.nextLine() + "' AND C.cid = T.cid and T.tid = P.tid AND S.sid = P.sid AND M.mvid = S.mvid AND S.sdate >= '";
			
		System.out.println("Please enter the start date in the following format: dd/mm/yyyy");
		command += scan.nextLine() + "' AND S.sdate <= '";
		
		System.out.println("Please enter the end date in the following format: dd/mm/yyyy");
		command += scan.nextLine() + "';";
		
		try {
            esql.executeQueryAndPrintResult(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
		
		//m.title,cid, date range 
	}

	public static void ListBookingInfoForUser(Ticketmaster esql){//14
		//
		//SELECT DISTINCT M.title, S.sdate, S.sttime, T.tname, C.sno FROM bookings B, movies M, shows S, theaters T, cinemaseats C, plays P  WHERE B.email = 'albertoscarlett@gmail.com' AND S.sid = B.bid AND P.tid = T.tid AND S.sid = P.sid;
		
		String command = "SELECT DISTINCT M.title, S.sdate, S.sttime, T.tname, C.sno FROM bookings B, movies M, shows S, theaters T, cinemaseats C, plays P  WHERE B.email = '";
		Scanner scan = new Scanner(System.in);
		String user;
		
		System.out.println("Please input the user's email");
		command += scan.nextLine() + "' AND S.sid = B.bid AND P.tid = T.tid AND S.sid = P.sid;";
		
		try {
            esql.executeQueryAndPrintResult(command);
		} catch(Exception e) {
			System.err.println (e.getMessage ());
    	}
	}
	
}