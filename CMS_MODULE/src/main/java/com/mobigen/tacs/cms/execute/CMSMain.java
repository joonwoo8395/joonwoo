package com.mobigen.tacs.cms.execute;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.mobigen.tacs.cms.ftp.FtpManage;

public class CMSMain {
	public final static Logger logger = Logger.getLogger(CMSMain.class);
	public static void main(String[] args) {
		CMSClient client = new CMSClient();
		try {
			client.run();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

//		FtpClient client;
//		try {
//			client = new FtpClient("90.90.90.248", 21);
//			client.ftpFileDownload("etcuser", "rhsl70^^", "/CMS_NAS/data/dumpdata/CM_ENB_ALL_V61_20190621144848.dat");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
}
