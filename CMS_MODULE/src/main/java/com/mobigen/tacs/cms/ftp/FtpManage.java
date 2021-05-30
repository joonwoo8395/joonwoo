

package com.mobigen.tacs.cms.ftp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.mobigen.tacs.cms.iris.IRISMakeFile;
import com.mobigen.tacs.cms.iris.IRISManager;

public class FtpManage {

	private FTPClient client = null;
	public static final Logger logger = Logger.getLogger(FtpManage.class);

	public FtpManage(String host, int port) throws SocketException, IOException {
		logger.info("FTP Start!!");

		client = new FTPClient();
		client.connect(host, port);

		int reply = client.getReplyCode();

		logger.info("Reply Code : " + reply);

		if (!FTPReply.isPositiveCompletion(reply)) {
			try {
				client.disconnect();

			} catch (IOException ie) {
				logger.error(ie);
			}
		} 

	}

	public void ftpDisconnect() throws IOException {
		if (client != null && client.isConnected()) {
			client.logout();
			client.disconnect();
		}
	}

	public void ftpConnected() {
		logger.info("FTP Connect : " + client.isConnected());
	}

	public void ftpFileUpload(String path, String fileDate) {

		String finFileName = String.format("%s_%s_%s.FIN", System.getProperty("service_code"), fileDate, System.getProperty("system"));

		logger.info("Upload File Name : " + finFileName);
		FileInputStream fis 	= null;

		try {			
			logger.info("dir : " + client.printWorkingDirectory());
			
			boolean changeFlag = client.changeWorkingDirectory(path);
			Path newFilePath 	= Paths.get(System.getProperty("tacs_ftp_dir"), finFileName);
//			Path newFilePath 	= Paths.get("/home/tacs/user/KimJW/CMS/DownFiles", "TRAP_CM_LTE_ENB_INFO_V61_20190621144848_TCSTCS01.FIN");
			
			Path filePath 		= Files.createFile(newFilePath);
			fis = new FileInputStream( filePath.toFile() );
			
			boolean uploadSuccess = client.storeFile(finFileName, fis);

			if (uploadSuccess) {
				logger.info("File Upload Success");

			} else {
				logger.info("File Upload Fail");
			}
		} catch (IOException ie) {
			logger.error(ie.getMessage(), ie);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}

	}

	public boolean ftpClientConnected() {
		int reply = client.getReplyCode();

		if (!FTPReply.isPositiveCompletion(reply)) {
			try {
				client.disconnect();

			} catch (IOException ie) {
				logger.error("disconnect Fail", ie);
			}
			return false;
		}

		return true;

	}

	public void ftpFileDownload(String user, String pwd, String filePath) throws IOException {
		File resultFile = new File(filePath);
		String path		= resultFile.getParent().toString();
		String fileName = resultFile.getName();
		String fileCreateDate = "";

		if (!ftpClientConnected()) {
			logger.error("Connection Fail");
			return;
		}

		try {
			client.login(user, pwd);

			client.setControlEncoding("UTF-8");

			client.setFileType(FTP.BINARY_FILE_TYPE);
			client.enterLocalPassiveMode();

			logger.info("dir : " + client.printWorkingDirectory());

			boolean changeFlag = client.changeWorkingDirectory(path);

			if (changeFlag) {
				logger.info("Dir Change Success " + changeFlag);

				try ( InputStream is = client.retrieveFileStream(fileName)
						; BufferedWriter bw = new BufferedWriter(new OutputStreamWriter
								(new FileOutputStream
										(new File(System.getProperty("tacs_ftp_dir"), fileName)), StandardCharsets.UTF_8))){
//										(new File("/home/tacs/user/KimJW/CMS/DownFiles", fileName)), StandardCharsets.UTF_8))){
					logger.info("filePath : " + filePath);
					logger.info("fileName : " + fileName);
					
					fileCreateDate = fileName.split("_")[4].split("[.]")[0];

					IOUtils.copy(is, bw, "EUC-KR");
					boolean fileTransferCheck = client.completePendingCommand();
					
					if ( fileTransferCheck ) {
						logger.info("File Download Success");
					} else {
						logger.error("File Download Fail");
					}
					
					ftpFileUpload(path, fileCreateDate);
					
				} catch(IOException ie) {
					logger.error("File Download Fail");
					logger.error(ie.getMessage(), ie);
				} finally {
					IRISMakeFile irisMakeFiles = new IRISMakeFile();
					
					String ctlFilePath = irisMakeFiles.makeCtlFile(System.getProperty("table_column"));
					String datFilePath = irisMakeFiles.makeDatFile(String.join("/", System.getProperty("tacs_ftp_dir"), fileName));
//					String ctlFilePath = irisMakeFiles.makeCtlFile("OPR_STATUS,C_UID,MME_GRP_ID,ENB_ID,BTS_NAME,REGION_ID,SISUL_CODE,VENDOR_ID,BRANCH_ID,BRANCH_NAME,OPTEAM_ID,OPTEAM_NAME,MTSO_TEAM_ID,MTSO_TEAM_NAME,COMPANY_ID,PART_ID,PART_NAME,INSTALL_DATE,UPDATE_DATE,BTS_TYPE,BTS_FORMAT,SERVICE_TYPE,CARD_TYPE,CELL_TYPE,CARD_NUM,POST1,POST2,CITY,GU,DONG,BUNGI,BUNGI1,BUNGI2,BUNGI3,ALTITUDE,HEIGHT,LATITUDE,LONGITUDE,LATITUDE_NORM,LONGITUDE_NORM,GPS_FLAG,AREA_INFO,MAOPERATOR,MOBILE_NUM,TEL_NUM,FAX_NUM,MZCOMPANY,IN_FLOOR,TOT_FLOOR,SERVICE_FLOOR,UPDATE_USER_ID,ENB_GROUP,EMS_ID,MASTER_IP,VLAN_INFO,SWITCH_INFO,MTSO_NAME,SERIALNUMBER,IMPORTANT_FLAG,GY_TYPE,GY_SISUL_CODE,GY_NAME,CA_TYPE,MC_TYPE,ONM_IP,ROAD_NAME,BASEMENT_INFO,BD_MAIN_NUM,BD_SUB_NUM,BD_NAME,ROAD_DONG,ROAD_FLOOR,ROAD_HO,ROAD_LAW_DONG,ROAD_LAW_RI,WIDE_BAND,MASTER_IP_2,MASTER_IP_3,MASTER_IP_4,IMP_GUBUN,LATITUDE_NORM_2,LONGITUDE_NORM_2,PKG_GROUP,EMS_BTS_NAME,EMS_NAME,VLAN_INFO2,SWITCH_INFO2,DU_VERSION,IMP_GUBUN1,IMP_GUBUN2,IMP_GUBUN3,MAC_ADDRESS,FREQUENCY,SGRP,TA,ITME_GUBUN,VLAN_INFO3,SWITCH_INFO3,VLAN_INFO4,SWITCH_INFO4,ETC_ITEM_01");
//					String datFilePath = irisMakeFiles.makeDatFile(String.join("/", "/home/tacs/user/KimJW/CMS/DownFiles", fileName));
				
					IRISManager irisConn = new IRISManager();
				
					Connection conn = irisConn.connect();
					
					irisConn.deleteData(conn, "TACS.CMS_DU_INFO");
					irisConn.loadData(conn ,"TACS.CMS_DU_INFO", ctlFilePath, datFilePath);
					/*
					try {
						fileDelete(System.getProperty("tacs_ftp_dir"), fileCreateDate);
					} catch (ParseException pe) {
						logger.error(pe.getMessage(), pe);
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
					*/
				}

			} else {
				logger.error("--Dir Change Fail!!--");
			}
		} catch ( Exception e ) {
			logger.error(e.getMessage(), e);

		} finally {
			ftpDisconnect();
		}
	}
	
	public void fileDelete(String filePath, String fileCreateDate) throws ParseException {
		DateFormat createFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		DateFormat dateFormat 	= new SimpleDateFormat("yyyyMMddHH");
		
		Date date 			= createFormat.parse(fileCreateDate);
		Calendar calendar 	= Calendar.getInstance();
		
		calendar.setTime(date);
		calendar.add(Calendar.HOUR, -1);
		
		String createDate	= dateFormat.format(date);
		String noDelDate	= dateFormat.format(calendar.getTime());
		
		for ( File oneFile : new File(filePath).listFiles() ) {
			String filename = oneFile.getName();
			if ( filename.contains(noDelDate) == false || filename.contains(createDate) == false ) {
				boolean isDeleted = oneFile.delete();
				logger.info(String.format("deleteFilename:%s, result:%s", filename, isDeleted));
			}
		}
		/*
		File file 			= new File(filePath);
		String fileList[] 	= file.list(); 
		  
		for (String file : fileList) {
			if ( !( file.contains(noDelDate) || file.contains(createDate) ) ) {
				File deleteFile = new File(filePath.concat(File.separator).concat(files));
				boolean isDeleted = deleteFile.delete();
				logger.info(String.format("deleteFilename:%s, result:%s", deleteFile.getName(), deleteFile.delete()));
				deleteFile.delete();
			}
		}
		*/
	}
}