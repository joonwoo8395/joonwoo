package com.mobigen.tacs.cms.entity;

import java.util.Properties;

public class CMSProperties {
//	private String CMS_IP;
//	private int CMS_PORT;
//	private int FTP_PORT;
//	private String CMS_USER;
//	private String CMS_PWD;
//      
//	private String SOURCE_GROUP;
//	private String SOURCE_KEY;
//	private String CMD_ANS;
//	private String CMD_HBC;
//	private String CMD_TRAP;
//	private String TARGET_GROUP;
//	private String TARGET_KEY;
//	private String TRAP_CODE;
//      
//	private String CMD_NTI;
//	private String CMD_DRG;
//	private String CMD_REG;
//	private String TIME_OUT;
//      
//	private String SERVICE_CODE;
//	private String FTP_SERVICE_CODE;
//
//	private String TACS_FTP_DIR;
//	private String SYSTEM;
//	private String SERVICECODE;
//
//	private String CMS_FTP_DIR;
//	private String CMS_FILE_NAME;
//
//	private String IRIS_URL;// =jdbc:iris:192.168.100.40:5050/TACS
//	private String TABLE;
//	private String KEY;
//	private String PARTITION;
//	private String CTL_FILE_PATH;
//	private String DAT_FILE_PATH;

	public CMSProperties() {

	}

	public void settingProp(Properties properties) {
		System.setProperty("cms_ip", properties.getProperty("cms_ip"));
		System.setProperty("cms_port", properties.getProperty("cms_port"));
		System.setProperty("ftp_port", properties.getProperty("ftp_port"));
		System.setProperty("cms_user", properties.getProperty("cms_user"));
		System.setProperty("cms_pwd", properties.getProperty("cms_pwd"));
		System.setProperty("source_group", properties.getProperty("source_group"));
		System.setProperty("source_key", properties.getProperty("source_key"));
		System.setProperty("cmd_ans", properties.getProperty("cmd_ans"));
		System.setProperty("cmd_hbc", properties.getProperty("cmd_hbc"));
		System.setProperty("cmd_trap", properties.getProperty("cmd_trap"));
		System.setProperty("target_group", properties.getProperty("target_group"));
		System.setProperty("target_key", properties.getProperty("target_key"));
		System.setProperty("trap_code", properties.getProperty("trap_code"));
		System.setProperty("cmd_nti", properties.getProperty("cmd_nti"));
		System.setProperty("cmd_drg", properties.getProperty("cmd_drg"));
		System.setProperty("cmd_reg", properties.getProperty("cmd_reg"));
		System.setProperty("time_out", properties.getProperty("time_out"));
		System.setProperty("service_code", properties.getProperty("service_code"));
		System.setProperty("ftp_service_code", properties.getProperty("ftp_service_code"));
		System.setProperty("tacs_ftp_dir", properties.getProperty("tacs_ftp_dir"));
		System.setProperty("system", properties.getProperty("system"));
		System.setProperty("iris_url", properties.getProperty("iris_url"));
		System.setProperty("iris_user", properties.getProperty("iris_user"));
		System.setProperty("iris_passwd", properties.getProperty("iris_passwd"));		
		
		System.setProperty("table", properties.getProperty("table"));
		System.setProperty("key", properties.getProperty("key"));
		System.setProperty("partition", properties.getProperty("partition"));
		System.setProperty("ctl_file_path", properties.getProperty("ctl_file_path"));
		System.setProperty("dat_file_path", properties.getProperty("dat_file_path"));
		System.setProperty("startDate_path", properties.getProperty("startDate_path"));
		System.setProperty("date_format", properties.getProperty("date_format"));
		System.setProperty("table_column", properties.getProperty("table_column"));
	}
}
