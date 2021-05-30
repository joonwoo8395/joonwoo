package com.mobigen.tacs.cms.execute;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.mobigen.tacs.cms.entity.CMSProperties;
import com.mobigen.tacs.cms.handler.CommandHandler;
import com.mobigen.tacs.cms.handler.ConfigHolder;
import com.mobigen.tacs.cms.handler.TRAPCommandHandler;
import com.mobigen.tacs.cms.message.BusMessage;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class CMSClient implements SignalHandler {
	public final static Logger logger = Logger.getLogger(CMSClient.class);
//	private final static String propertyPath = "/home/tacs/user/KimJW/CMS/conf/cmsClient.properties";
	private final static String propertyPath = "/home/tacs/user/KimJW/CMS/conf/cmsClient.properties";
	private CMSClientSessionHandler client;
	private CommandHandlerExecutor executor;
	private BlockingQueue<Object> commandHandelrQ = new ArrayBlockingQueue<Object>(10);
	private ConfigHolder ch = new ConfigHolder();

	public CMSClient() {

		Signal.handle(new Signal("INT"), this);
		Signal.handle(new Signal("TERM"), this);
	//	Signal.handle(new Signal("HUP"), this);
	}

	public BusMessage initExec() throws FileNotFoundException, IOException {
		BusMessage message = new BusMessage();
		logger.info("init Execute!!");

		String startDate 	= null;
		String nowDate 		= null;
		
		startDate 	= IOUtils.toString( new FileInputStream(System.getProperty("startDate_path")), "UTF-8" );

		nowDate		= DateFormatUtils.format( new Date(), "yyyyMMddHHmmss" );
		logger.info( "StartDate : " + startDate );
		logger.info( "EndDate   : " + nowDate );

		message = trapHandler(startDate, nowDate);
		
		
		return message;
	}

	public BusMessage trapHandler(String startDate, String endDate) {
		CommandHandler trapHandler = new TRAPCommandHandler(startDate, endDate);
		return trapHandler.makeAckMessage("OK");
	}

	public void run() throws IOException {
		logger.info("CMS Module Start !!");
		readProp();
		ch.load();
			
		String host = System.getProperty("cms_ip");
		int port	= Integer.parseInt(System.getProperty("cms_port"));
			
		logger.debug("CMS IP : " + host ); 
		logger.debug("CMS PORT : " + port );
			
		client = new CMSClientSessionHandler(host, port, commandHandelrQ, ch);

		client.connect();
			
		if ( client.isConnected() ) {
			logger.debug("Session Connected!!");
			
//			try {
//				BusMessage message = initExec();
//				client.initData(message);
//			} catch (FileNotFoundException fne) {
//				logger.error(fne.getMessage(), fne);
//			} catch (IOException ie) {
//				logger.error(ie.getMessage(), ie);
//			}
			
			executor = new CommandHandlerExecutor( this.commandHandelrQ );
			executor.start();
		} else {
			logger.info( "Session not Connected" );
		}

	}

	public void readProp() throws IOException {
		Properties properties = new Properties();
		
//		InputStream is = CMSClient.class.getClassLoader().getResourceAsStream("cmsClient.properties");
		InputStream is = new FileInputStream("/home/tacs/user/KimJW/CMS/conf/cmsClient.properties");
		properties.load(is);
		CMSProperties cmsProperties = new CMSProperties();
		cmsProperties.settingProp( properties );
			
//		System.out.println("---------------"+System.getProperty("startDate_path"));
		
		
		
//		try ( Reader fr = new FileReader(propertyPath) ) {
//			properties.load(fr);
//
//		} catch (IOException ie) {
//			logger.error( ie.getMessage(), ie );
//
//		}

	}

	@Override
	public void handle(Signal signal) {
		logger.info(signal);
		client.disconnect();
		/*
		 * if (signal.getName() == "HUP" || signal.getName() == "INT" ||
		 * signal.getName() == "KILL" || signal.getName() == "QUIT") {
		 * 
		 * SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss"); Date
		 * time = new Date(); String nowDate = dateFormat.format(time);
		 * 
		 * try (FileWriter fw = new
		 * FileWriter(String.format("/home/tacs/user/KimJW/CMS/startDate.txt"))) {
		 * fw.write(nowDate);
		 * 
		 * } catch (IOException e) { logger.error(e.getMessage(), e);
		 * 
		 * } }
		 * 
		 * System.exit(0);
		 */
	}

}