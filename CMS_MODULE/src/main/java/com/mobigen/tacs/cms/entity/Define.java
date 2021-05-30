package com.mobigen.tacs.cms.entity;

import java.util.Properties;

public class Define {
	
	public static Properties property;
	
	public static final String CMS_IP = "90.90.90.248";
	// public static final String CMS_IP = "150.23.15.20";
	public static final int CMS_PORT = 9980;
	public static final int FTP_PORT = 21;
	public static final String CMS_USER = "etcuser";
	public static final String CMS_PWD = "rhsl70^^";
	// public static final String SOURCE_GROUP = "BUS";
	// public static final String SOURCE_KEY = "BUS05";
	public static final String SOURCE_GROUP = "TCS";
	public static final String SOURCE_KEY = "TCS01";
	public static final String CMD_ANS = "ANS";
	public static final String CMD_HBC = "HBC";
	public static final String CMD_TRAP = "TRP";
	public static final String TARGET_GROUP = "CMS";
	public static final String TARGET_KEY = "CMS";
	public static final String TRAP_CODE = "TRAP_HIS";


	public static final String CMD_NTI = "NTI";
	public static final String CMD_DRG = "DRG";
	public static final String CMD_REG = "REG";
	public static final int TIME_OUT = 5000;

	public static final String SERVICE_CODE = "BTS";
	public static final String FTP_SERVICE_CODE = "TRAP_T1,TRAP_T2,TRAP_T3";

	public static final String TACS_FTP_DIR = "/home/tacs/user/KimJW/CMS";
	public static final String SYSTEM = "LBSLBS01";
	public static final String SERVICECODE = "GET_BTS_SQL";

	public static final String CMS_FTP_DIR = "/home/eva/data/dumpdata";
	public static final String CMS_FILE_NAME = "TEST";

	public static final String IRIS_URL = "jdbc:iris://192.168.100.40:5050/TACS";
	public static final String TABLE = "";
	public static final String KEY = "";
	public static final String PARTITION = "";
	public static final String CTL_FILE_PATH = "";
	public static final String DAT_FILE_PATH = "";

}
