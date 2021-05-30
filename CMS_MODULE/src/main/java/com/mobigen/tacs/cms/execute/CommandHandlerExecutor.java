package com.mobigen.tacs.cms.execute;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.mobigen.tacs.cms.handler.CommandHandler;


public class CommandHandlerExecutor extends Thread {
	public final static Logger logger = Logger.getLogger(CommandHandlerExecutor.class);
	private BlockingQueue<Object> que = null;
	public CommandHandlerExecutor(BlockingQueue<Object> q){
		this.que = q;
	}
	public void run(){
		CommandHandler handler = null;
		logger.info("executor start!!");
		while(true) {
			try {
				handler = (CommandHandler)this.que.take();
			
				if (handler != null)		
					handler.run();
				Thread.sleep(100);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		
		}
	}
}