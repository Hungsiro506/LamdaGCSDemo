package com.gcs.batchcore.batchlayer.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.gcs.batchcore.utils.HiveConfiguration;
/**
 *@author hungdv
 *@since 7-8-2015 
 */
public class HiveService  {
	
	// Attribute - properties:
	//private static final Logger LOG = Logger.getLogger(HiveServiceImp.class);
	private String host;
	private String port;
	private Connection conn;
	private String user;
	private String password;
	private String dbName;
	
	// Methods :
	public String getHost()
	{
		return this.host;
	}
	public void setHost(String pHost){
		this.host = pHost;
	}
	public void setPort(String pPort){
		this.port = pPort;
	}
	public String getPort(){
		return this.port;
	}
	public void setConnection(Connection pConn){
		this.conn = pConn;
	}
	public Connection getConnection(){
		return this.conn;
	}
	public String getUser(){
		return this.user;
	}
	public void setUser(String pUser){
		this.user = pUser;
	}
	public String getPassword(){
		return this.password;
	}
	public void setPassword(String pUser){
		this.password = pUser;
	}
	public String getDbName(){
		return this.dbName;
	}
	public void setDbName(String pUser){
		this.dbName = pUser;
	}
	public void openConnection() {
		try{
		 System.out.println("open connection");
		 this.conn = DriverManager.getConnection("jdbc:hive2://" + getHost()
	                + ":" + getPort() + "/" + getDbName() , getUser(), getPassword());
	     System.out.println("open connection success");
		}catch(SQLException ex){
			ex.printStackTrace();
		}
	}
	public void closeConnection() {

		try{
			if(conn!=null)
				conn.close();
			}catch(Exception ex){
			ex.printStackTrace();
			System.exit(1);
			}
	}
	public HiveService(String host, String port, String database, String user, String password) {
        
        this.setHost(host);
        this.setPort(port);
        this.setDbName(database);
        this.setUser(user); // default is ""
        this.setPassword(password); // default is ""
        System.out.println("meow");
        try {
            String driverName = HiveConfiguration.DRIVER_NAME;
            Class.forName(driverName);
        	} catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
	
	// Create table
	public boolean createTable(String tableName, List<String> columns, List<String> attributes) throws SQLException {
        if ((columns.size() != attributes.size()) || columns.size() <= 0||tableName == null) { 
            return false;
        }
        // query to create table
        String strQuery = "CREATE TABLE IF NOT EXISTS " + tableName;
        String strCreateCol = toCreateColQL(columns, attributes);
        strQuery = strQuery + " (" + strCreateCol + " )";
        return execute(strQuery);
    }
    // Create Database 
	public boolean createDatabase(String database) throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + database;
        if(database!= null){
        return execute(sql);
        }
        return false;
    }
    // Remove DataBase
	public boolean removeDatabase(String database) throws SQLException {
        String sql = "DROP DATABASE " + database;
        if(database!= null){
          return execute(sql);
          }
          return false;
    }
    // CREATE QUERY TO CREATE TALBE COLUMN 
    protected String toCreateColQL(List<String> columns, List<String> attributes) {
        // query  to create table
        String strCreateCol = ""; // Init columns & attributes
        for (int i = 0; i < columns.size(); i++) {
            strCreateCol += columns.get(i) + " " + attributes.get(i) + ",";
        }
        strCreateCol = strCreateCol.substring(0, strCreateCol.length() - 1); // Remove end character ","
        return strCreateCol;
	}
    // Execute a scala query
    public boolean execute(String strQuery) throws SQLException {
      if(strQuery != null ){
        Statement stmt = this.conn.createStatement();
        return stmt.execute(strQuery);
      }
      else return false;
    }
	// Return a result set contain query result. 
    public ResultSet executeQuery(String strQuery) throws SQLException {
      if(strQuery !=  null){
        PreparedStatement stmt = this.conn.prepareStatement(strQuery);
        return stmt.executeQuery();
      }
      else return null;
    }
    // Describe table
    public ResultSet getSchemaColumns(String tableName) throws SQLException {
      if(tableName != null){
        String strQuery = "DESCRIBE " + tableName;
        return executeQuery(strQuery);
      }
      else return null;
    }
    // Insert table
    public boolean insertTable(String tableName, String strQueryData) throws SQLException {
      if(strQueryData != null){  
      String strQuery = "INSERT OVERWRITE TABLE " + tableName + " " + strQueryData;     
        return execute(strQuery);
      }
      else return false;
    }
    // Delete table
    public boolean deleteTable(String tableName) throws SQLException {
        if(tableName != null){
          String strQuery = "DROP TABLE " + tableName;
          return execute(strQuery);
        }
        else return false;
    }
    public void executeNonQuery(String sql)throws SQLException{
      if(sql != null){
    	PreparedStatement stmt = this.conn.prepareStatement(sql);
        stmt.execute();
      }
    }
    public void executeInsertQuery(String sqlQuery,String cond)throws SQLException{
      if(sqlQuery != null || cond != null){
    	PreparedStatement stmt = this.conn.prepareStatement(sqlQuery);
    	stmt.setString(1, cond);
    	stmt.execute();
      }
    }


}
