package com.mobigen.tacs.cms.handler;

import com.mobigen.tacs.cms.message.BusMessage;

public class HBCCommandHandler extends CommandHandler{
	public BusMessage makeAckMessage(String dataCheck){
		return this.makeMessage(System.getProperty("cmd_hbc")
				,System.getProperty("source_group")
				,System.getProperty("source_key")
				, ""
				, ""
				, ""
				, ""
				, ""
				, this.getDateTime() 
				, String.format("HBC_%s", dataCheck));
		
	}
	public void processMessage() {
		logger.info("HBC is not message!!");
	}
}
