package com.mobigen.tacs.cms.handler;

import java.io.IOException;
import java.net.SocketException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mobigen.tacs.cms.ftp.FtpManage;
import com.mobigen.tacs.cms.message.BusMessage;

public class NTICommandHandler extends CommandHandler {
	private final int COLUMN_DATA_IDX = 0;
//	private final int FTP_FILE_DIR_INDEX = 0;
//	private final int FTP_FILE_NAME_INDEX = 1;

	public BusMessage makeAckMessage(String dataCheck) {
		logger.info("message : " + this.mParsedJsonMessage.toString());
		String messageIdNode = (String) this.mParsedJsonMessage.get("messageId");

		return this.makeMessage(System.getProperty("cms_ans"), System.getProperty("source_group"),
				System.getProperty("source_key"), "", "", messageIdNode, "", System.getProperty("service_code"),
				this.getDateTime(), String.format("NTI_%s", dataCheck));
	}
	
	public void processMessage() throws NumberFormatException, SocketException, IOException {
		logger.info("NTI message : " + this.mMessage.getmValue());
		logger.info("serviceCode : " + mParsedJsonMessage.get("serviceCode"));
		logger.info("ftp_service_code : " + System.getProperty("ftp_service_code"));
		
		if (System.getProperty("ftp_service_code").equals(mParsedJsonMessage.get("serviceCode"))) {
			JsonParser jsonParse = new JsonParser();
			JSONObject jsonMsg = jsonParse.jsonParser(this.mMessage.getmValue());
			JSONObject msgData = jsonParse.jsonParser(String.valueOf(jsonMsg.get("data")));

			FtpManage ftp = null;
			
			ftp = new FtpManage(System.getProperty("cms_ip"), Integer.parseInt(System.getProperty("ftp_port")));
			

			
			JSONArray jsonDataArr = (JSONArray) msgData.get("COLUMN_DATA");
			
			logger.info(jsonDataArr.get(COLUMN_DATA_IDX));
			
			ftp.ftpFileDownload(System.getProperty("cms_user"), System.getProperty("cms_pwd"),
					String.valueOf(jsonDataArr.get(COLUMN_DATA_IDX)));

		} else {
			logger.info("Service Code : " + mParsedJsonMessage.get("serviceCode"));
			logger.info(String.format("ServiceCode : %s", mParsedJsonMessage.get("serviceCode")));
		}
	}
}