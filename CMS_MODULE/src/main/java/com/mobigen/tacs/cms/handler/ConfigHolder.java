package com.mobigen.tacs.cms.handler;

import java.util.HashMap;
import java.util.Map;


public class ConfigHolder {
	private Map<String, String> mConfig = new HashMap<String,String>();
	private Map<String, String> mFtpServiceCodeMap = new HashMap<String, String>();
	
	public Boolean load(){
		this.registerParseServiceCode(System.getProperty("service_code"), "SERVICE_CODE");
		this.registerParseServiceCode(System.getProperty("ftp_service_code"), "FTP");
		return true;
	}
	
	public Boolean checkFtpServiceCode(String svcCode){
		return (this.mFtpServiceCodeMap.get(svcCode) != "");
	}
	public String get(String key){
		return this.mConfig.get(key);
	}
	
	private void registerParseServiceCode(String str, String code){
		String []ftpCode = str.split(",");
		for (int i = 0; i< ftpCode.length; i++ ) {
			this.registerServiceCode(code, ftpCode[i], ftpCode[i]);
		}	
	}
	private void registerServiceCode(String code, String key, String value) {
		key.trim();
		value.trim();
		if (code.equals("SERVICE_CODE")) {
			this.mConfig.put(key, value);
		}
		else if (code.equals("FTP")) {
			this.mFtpServiceCodeMap.put(key, value);	
		}
	}
}
