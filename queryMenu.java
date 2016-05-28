import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;

/**
   Executes all SQL statements from a file or the console.
*/
public class ExecSQL
{
   private final static String [] types = { "INTEGER", "REAL", "DOUBLE", "DECIMAL(m, n)", "BOOLEAN", "VARCHAR(n)", "CHARACTER(n)"};
   
   public static void main(String[] args) 
         throws SQLException, IOException, ClassNotFoundException
   {   
      if (args.length == 0)
      {   
         System.out.println(
               "Usage: java -classpath driver_class_path(derby.jar)"
               + File.pathSeparator 
               + ". ExecSQL propertiesFile");
         return;
      }

      SimpleDataSource.init(args[0]);
      
      Scanner in = new Scanner(System.in);
      
      boolean status = true;

      while(status){
         String table = "";
         System.out.println("\nWhat command would you like to perform:\n");
         System.out.println("<1> CREATE TABLE");
         System.out.println("<2> INSERT INTO");
         System.out.println("<3> DROP");
         System.out.println("<4> DELETE");
         System.out.println("<5> UPDATE");
         System.out.println("<6> SELECT");
         System.out.println("<7> VIEW TABLES");
         System.out.println("<8> HELP");
         System.out.println("<9> EXIT");
         int choice = in.nextInt();
         in.nextLine();
         if(choice > 1 && choice < 10 && choice != 7){
            System.out.println("What table would you like to work with: ");
            table = in.nextLine().trim();
         }
         else if (choice < 0 || choice > 9){
            System.out.println("Enter a number greater than 0 and less than 10");
            continue;
         }
         switch(choice){
            case 1:
               createTable();
               break;
            case 2:
               insertIntoTable(table);
               break;
            case 3:
               dropTable(table);
               break;
            case 4:
               deleteTable(table);
               break;
            case 5:
               updateTable(table);
               break;
            case 6:
               selectTable(table);
               break;
            case 7:
               viewTables();
               break;
            case 8:
               help();
               break;
            case 9:
               status = false;
               break;
         }
      }
   }

   public static void createTable()throws SQLException, IOException, ClassNotFoundException{
      Scanner in = new Scanner(System.in);
      System.out.println("What would you like to name your table: ");
      String name = in.nextLine().trim();
      if(!checkTable(name)){
         System.out.println("The variable types are as follows..");
         printTypes();
         String command = "CREATE TABLE "+name +"(";
         System.out.println("Enter the column name and the type and the lengths if required (All separated by spaces)");
         System.out.println("Enter -1 to stop adding columns");
         while(true){
            String column = in.nextLine().trim();
            if(column.equals("-1")){
               break;
            }
            String components [] = column.split(" ");
            command += components[0];
            if(components.length == 3){
               components[1] = components[1]+"("+components[2]+")";
            }
            if(components.length == 4){
               components[1] = components[1]+"("+components[2]+","+components[3]+")";
            }
            command += " " + components[1].toUpperCase() + ", ";
            System.out.println("COMMAND SO FAR: "+command);
         }
         command = command.substring(0,command.length()-2)+")";
         System.out.println(command);
         executeCommand(command);
      }
      else{
         System.out.println("no table with that name exists");
      }
   }

   public static void insertIntoTable(String name)throws SQLException, IOException, ClassNotFoundException{
      Scanner in = new Scanner(System.in);
      String command = "INSERT INTO " + name + "\n" + "VALUES (";
      System.out.println("Enter values as they appear in SELECT command. Add single quotes for strings");
      System.out.println("Enter -1 to stop adding values");
      while(true){
         String value = in.nextLine().trim();
         if(value.equals("-1")){
            break;
         }
         command += value + ", ";
      }
      command = command.substring(0, command.length()-2)+ ")";
      System.out.println(command);
      executeCommand(command);
   }

   public static void dropTable(String name)throws SQLException, IOException, ClassNotFoundException{
      executeCommand("DROP TABLE "+name);
   }

   public static void deleteTable(String name)throws SQLException, IOException, ClassNotFoundException{
      Scanner in = new Scanner(System.in);
      String command = "DELETE FROM "+ name + "\n" + "WHERE ";
      System.out.println("Enter a true false statement to evaluate what to delete");
      command += " " + in.next().trim();
      executeCommand(command);
   }

   public static void updateTable(String name)throws SQLException, IOException, ClassNotFoundException{
      Scanner in = new Scanner(System.in);
      String command = "UPDATE "+ name + "\n" + "SET ";
      System.out.println("Enter reassignments (use single quotes for strings)");
      while(true){
         String assignment = in.nextLine().trim();
         if(assignment.equals("-1")){
            break;
         }
         command += assignment + ", ";
      }
      command= command.substring(0, command.length()-2);
      System.out.println();
      System.out.println("Would you like to add WHERE parameters?");
      if(in.nextLine().trim().toLowerCase().equals("yes")){
         System.out.println("Enter parameters: ");
         command += " WHERE ";
         while(true){
            String assignment = in.nextLine().trim();
            if(assignment.equals("-1")){
               break;
            }
            command += assignment + " AND ";
         }
         command = command.substring(0, command.length()-5);
      }
      System.out.println(command);
      executeCommand(command);
   }

   public static void selectTable(String name)throws SQLException, IOException, ClassNotFoundException{
      Scanner in = new Scanner(System.in);
      String command = "SELECT ";
      System.out.println("Enter a column or a star");
      command+= in.nextLine().trim() + " " + "FROM "+ name;
      System.out.println(command);
      executeCommand(command);
   }

   public static void help(){
      //to be implemented
   }

   public static void viewTables()throws SQLException, IOException, ClassNotFoundException{
      try (Connection conn = SimpleDataSource.getConnection())
      { 
         DatabaseMetaData md = conn.getMetaData();
         ResultSet rs = md.getTables(null, null, "%", null);
         while (rs.next()) {
           System.out.println(rs.getString(3));
         }
      }
   }

   public static void printTypes(){
      for(int i = 0 ;i < types.length; i++){
         System.out.println(types[i]);
      }
   }

   /**
      Prints a result set.
      @param result the result set
   */
   public static void showResultSet(ResultSet result) 
         throws SQLException
   { 
      ResultSetMetaData metaData = result.getMetaData();
      int columnCount = metaData.getColumnCount();

      for(int i = 0; i < columnCount; i++){
         System.out.print("----------");
      }
      System.out.println();
      for (int i = 1; i <= columnCount; i++)
      {  
         System.out.print("|");
         System.out.print(metaData.getColumnLabel(i));
         System.out.print(getSpaces(10-metaData.getColumnLabel(i).length()));
      }
      System.out.print("|");
      System.out.println();

      while (result.next())
      {  
         for (int i = 1; i <= columnCount; i++)
         {  
            System.out.print("|");
            System.out.print(result.getString(i));
            System.out.print(getSpaces(10-result.getString(i).length()));
         }
         System.out.print("|");
         System.out.println();
      }
      for(int i = 0; i < columnCount; i++){
         System.out.print("----------");
      }
      System.out.println();
   }

   public static void executeCommand(String command)throws SQLException, IOException, ClassNotFoundException{
      try (Connection conn = SimpleDataSource.getConnection();
            Statement stat = conn.createStatement())
      {     
         System.out.println("first try"); 
         try
            {
               System.out.println("second try");
               boolean hasResultSet = stat.execute(command);
               System.out.println("hasResultSet "+ hasResultSet);
               if (hasResultSet)
               {
                  try (ResultSet result = stat.getResultSet())
                  {
                showResultSet(result);
                  }
               }
            }
            catch (SQLException ex)
            {
               System.out.println(ex);
            }
      }
   }

   public static boolean checkTable(String command)throws SQLException, IOException, ClassNotFoundException{
      try (Connection conn = SimpleDataSource.getConnection();
            Statement stat = conn.createStatement())
      {      
         try
            {
               boolean hasResultSet = stat.execute("SELECT * FROM "+command);
               if (hasResultSet)
               {
                  return true;
               }
               return false;
            }
            catch (SQLException ex)
            {
               System.out.println(ex);
            }
      }
      return false;
   }

   public static String getSpaces(int num){
      String spaces = "";
      for(int i = 0; i < num; i++){
         spaces+=" ";
      }
      return spaces;
   }
}
