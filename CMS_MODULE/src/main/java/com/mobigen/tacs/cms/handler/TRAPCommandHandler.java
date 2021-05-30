package com.mobigen.tacs.cms.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.mobigen.tacs.cms.message.BusMessage;

public class TRAPCommandHandler extends CommandHandler {

	public String startDate = null;
	public String endDate = null;

	public TRAPCommandHandler(String startDate, String endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public BusMessage makeAckMessage(String dataCheck) {
		return this.makeMessage(System.getProperty("cmd_trap"), System.getProperty("source_group"), System.getProperty("source_key"), System.getProperty("target_group"),
				System.getProperty("target_group"), "", "Request", System.getProperty("trap_code"), this.getDateTime(), hisData(startDate, endDate));

	}

	public JSONObject hisData(String startDate, String endDate) {
		try {
			Map<String, Object> hashMap = new HashMap<String, Object>();
			Map<String, Object> hisMap = new HashMap<String, Object>();

			List<String> columnList = new ArrayList<String>();
			List<String> valueList = new ArrayList<String>();
			List<Object> dataList = new ArrayList<Object>();

			columnList.add("START_DATETIME");
			columnList.add("END_DATETIME");

			valueList.add(startDate);
			valueList.add(endDate);
			dataList.add(valueList);

			hashMap.put("COLUMN_HEADER", columnList);
			hashMap.put("COLUMN_DATA", dataList);

			hisMap.put("HIS", hashMap);

			JSONObject jsonData = new JSONObject(hisMap);

			return jsonData;
//			return hisMap.toString();

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	public void processMessage() {
		logger.info("TRAP is not message!!");
	}
}
