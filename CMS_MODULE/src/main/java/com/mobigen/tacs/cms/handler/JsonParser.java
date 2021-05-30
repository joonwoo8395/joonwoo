package com.mobigen.tacs.cms.handler;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonParser {

	public JSONObject jsonParser(String jsonStr) {
		JSONObject jsonObj = null;
		try {
			JSONParser parser = new JSONParser();
			jsonObj = (JSONObject) parser.parse(jsonStr);

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return jsonObj;
	}
}
