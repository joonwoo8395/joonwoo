package com.mobigen.tacs.cms.handler;

import java.io.IOException;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.json.simple.JSONObject;

import com.mobigen.tacs.cms.message.BusMessage;

public abstract class CommandHandler {
	public final static Logger logger = Logger.getLogger(CommandHandler.class);
	protected IoSession mSession;
	protected ConfigHolder mConfigHolder;
	protected BusMessage mMessage;
	protected JSONObject mParsedJsonMessage;
//	protected RequestResponseFilter reqresFilter;

	public CommandHandler() {
		this.mMessage = null;
		this.mParsedJsonMessage = null;
	}

	public void setConfigHolder(ConfigHolder configHolder) {
		this.mConfigHolder = configHolder;

	}

	public void setBusMessage(BusMessage bm) {
		this.mMessage = bm;
	}

	public void setIoSession(IoSession session) {
//		this.reqresFilter = new RequestResponseFilter(
//		                    new BasicRequestFactory(define.getTIME_OUT()));
		this.mSession = session;
	}

	public BusMessage makeMessage(String command, String sourceSessionGroup, String sourceSessionKey,
			String targetSessionGroup, String targetSessionKey, String messageId, String messageType,
			String serviceCode, String dateTime, JSONObject data) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		List<String> source = new ArrayList<String>();
		List<String> target = new ArrayList<String>();

		List<Object> tartgetList = new ArrayList<Object>();

		source.add(sourceSessionGroup);
		source.add(sourceSessionKey);

		target.add(targetSessionGroup);
		target.add(targetSessionKey);

		tartgetList.add(target);

		hashMap.put("sourceSessionKey", source);
		hashMap.put("targetSessionKey", tartgetList);
		hashMap.put("messageType", messageType);
		hashMap.put("messageId", messageId);
		hashMap.put("serviceCode", serviceCode);
		hashMap.put("dateTime", dateTime);
		hashMap.put("data", data);

		JSONObject json = new JSONObject(hashMap);
		BusMessage message = new BusMessage();

		logger.info("Create BusMessage : " + message);
		
		message.setLength(json.toString().length());
		message.setCommand(command);
		message.setmValue(json.toString());

		return message;
	}

	public BusMessage makeMessage(String command, String sourceSessionGroup, String sourceSessionKey,
			String targetSessionGroup, String targetSessionKey, String messageId, String messageType,
			String serviceCode, String dateTime, String data) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		List<String> source = new ArrayList<String>();
		List<String> target = new ArrayList<String>();

		source.add(sourceSessionGroup);
		source.add(sourceSessionKey);

		target.add(targetSessionGroup);
		target.add(targetSessionKey);

		hashMap.put("sourceSessionKey", source);
		hashMap.put("targetSessionKey", target);
		hashMap.put("messageType", messageType);
		hashMap.put("messageId", messageId);
		hashMap.put("serviceCode", serviceCode);
		hashMap.put("dateTime", dateTime);
		hashMap.put("result", data);

		JSONObject json = new JSONObject(hashMap);
		BusMessage message = new BusMessage();

		message.setLength(json.toString().length());
		message.setCommand(command);
		message.setmValue(json.toString());

		return message;
	}

	public void run() throws NumberFormatException, SocketException, IOException {
		String dataCheck = null;

		if (this.parseData()) {
			dataCheck = "OK";
			this.returnAck(this.makeAckMessage(dataCheck));
			this.processMessage();
		} else {
			dataCheck = "NOK";
			logger.info("invalid message format : " + this.mMessage.getmValue());
			this.returnAck(this.makeAckMessage(dataCheck));
		}
	}

	public String getDateTime() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();

		return dateFormat.format(date);
	}

	private boolean parseData() {
		JsonParser parse = new JsonParser();
		this.mParsedJsonMessage = parse.jsonParser(this.mMessage.getmValue());
		if (this.mParsedJsonMessage != null) {
			return true;
			
		} else {
			return false;
		}
	}

	private void returnAck(BusMessage message) {
		logger.info("send message : " + message);
		if (message != null) {
			this.mSession.write(message);
//			this.reqresFilter.query(this.mSession, message);
		}
	}

	public abstract BusMessage makeAckMessage(String dataCheck);

	abstract void processMessage() throws NumberFormatException, SocketException, IOException;
}
