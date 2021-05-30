package com.mobigen.tacs.cms.iris;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mobigen.iris.jdbc.IRISStatement;

public class IRISManager {

	public final static Logger logger = Logger.getLogger(IRISManager.class);
	private Connection conn = null;
	static private Properties info = null;
	private IRISStatement stmt = null;
	String url = null;
	
	public IRISManager() {
//		url = System.getProperty("iris_url");
		
		url = "jdbc:iris://90.90.90.200:5050/TACS";
		info = new Properties();

		info.setProperty("user", "tacs");
		info.setProperty("password", "tacs12#$");
		
//		info.setProperty("user", System.getProperty("iris_user"));
//		info.setProperty("password", System.getProperty("iris_passwd"));
		info.setProperty("direct", "false");
	}

	public Connection connect() {
		try {
			Class.forName("com.mobigen.iris.jdbc.IRISDriver");
			conn = DriverManager.getConnection(url, info);
			stmt = (IRISStatement) conn.createStatement();
			
		} catch (SQLException se) {
			se.printStackTrace();

		} catch (ClassNotFoundException cfe) {
			cfe.printStackTrace();
			
		}
		return conn;
	}

	public void countWrite(String result) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date date 		= new Date();
		String timeDate = format.format(date);
		
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter("/home/tacs/user/KimJW/CMS/DateFiles/dateFile.txt", true))){
			bw.append(String.format("%s : %s", timeDate, result));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void loadData(Connection conn, String table, String ctl_file_path, String dat_file_path) {
		String result = null;
		
		try {
			stmt.SetFieldSep("|");
			stmt.SetRecordSep("^-^");
			result = stmt.Load(table, null, null, ctl_file_path, dat_file_path);
		} catch (SQLException e) {
			e.printStackTrace();
		
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		countWrite(result);
		logger.info(result);
	}
	
	public void deleteData(Connection conn, String table) {
		String sql 		= "delete from " + table;
		boolean sqlBool = false;
		
		try {
			sqlBool = stmt.execute(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		
		if ( sqlBool ) {
			logger.info("Delete Success");
		} else {
			logger.error("Delete Fail!!");
		}
	}

	public void selectData(Connection conn) {
		try {

		    ResultSet rs = stmt.executeQuery(
		        "select * from TACS.CMS_DU_INFO;"
		    );

		    for( int i = 1 ; i <= rs.getRow() ; i++ ) {
		        rs.next();
		        System.out.println(
		            rs.getString(1)
		        );
		    }

		    rs.close();
		    stmt.close();
		    conn.close();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	}
}
