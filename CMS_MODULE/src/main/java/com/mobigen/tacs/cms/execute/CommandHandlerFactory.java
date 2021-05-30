package com.mobigen.tacs.cms.execute;

import org.apache.mina.core.session.IoSession;

import com.mobigen.tacs.cms.handler.CommandHandler;
import com.mobigen.tacs.cms.handler.ConfigHolder;
import com.mobigen.tacs.cms.handler.HBCCommandHandler;
import com.mobigen.tacs.cms.handler.NTICommandHandler;
import com.mobigen.tacs.cms.handler.REGCommandHandler;
import com.mobigen.tacs.cms.message.BusMessage;

public class CommandHandlerFactory {
	public CommandHandler createCommandHandler(ConfigHolder configHolder, BusMessage bm, IoSession session) {
		CommandHandler handler = null;
		
		String command = bm.getCommand();
		
		CMSClient.logger.info("command : " + command);
		
		if (command.equals(System.getProperty("cmd_reg"))) {
			CMSClient.logger.info("REG start");
			handler = new REGCommandHandler();
			
		} else if (command.equals(System.getProperty("cmd_hbc"))) {
			CMSClient.logger.info("HBC start");
			handler = new HBCCommandHandler();
			
		} else if (command.equals(System.getProperty("cmd_nti"))) {
			CMSClient.logger.info("NTI start");
			handler = new NTICommandHandler();
		}
		
		if (handler != null) {
			handler.setConfigHolder(configHolder);
			handler.setBusMessage(bm);
			handler.setIoSession(session);
//			System.out.println("handler : " + handler.getDateTime());
		}
		return handler;
	}
}
