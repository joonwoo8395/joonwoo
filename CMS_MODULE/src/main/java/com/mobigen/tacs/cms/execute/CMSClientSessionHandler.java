package com.mobigen.tacs.cms.execute;

import java.io.FileWriter;
import java.io.IOException;

//import com.adaptor.sync.*;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.json.simple.JSONObject;

import com.mobigen.tacs.cms.codec.SFProtocolCodecFactory;
import com.mobigen.tacs.cms.handler.CommandHandler;
import com.mobigen.tacs.cms.handler.ConfigHolder;
import com.mobigen.tacs.cms.message.BusMessage;

public class CMSClientSessionHandler extends IoHandlerAdapter {
	public final static Logger logger = Logger.getLogger(CMSClientSessionHandler.class);
	// private static final int defaultTimeout = 5000;
	private static final int defaultTimeout = 60 * 1000;
	// private static final int defaultIdleTime = 60;
	private int timeout = 3000;
	private String host;
	private int port;
	private BlockingQueue<Object> commandHandlerQ;
	private SocketConnector connector;
	private ConfigHolder configHolder;
	private IoSession session;

	public CMSClientSessionHandler(String host, int port, BlockingQueue<Object> commandHandlerQ, ConfigHolder ch) {
		this(host, port, CMSClientSessionHandler.defaultTimeout, commandHandlerQ, ch);
	}

	public CMSClientSessionHandler(String host, int port, int timeout, BlockingQueue<Object> cmdHQ, ConfigHolder ch) {
		this.host 			 = host;
		this.port 			 = port;
		this.timeout 		 = timeout;
		this.commandHandlerQ = cmdHQ;
		this.configHolder 	 = ch;
		
		this.connector = new NioSocketConnector();
		this.connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new SFProtocolCodecFactory(false)));

		this.connector.setHandler(this);
	}

	public boolean isConnected() {
		return (session != null && session.isConnected());
	}

	public void connect() {

		ConnectFuture connectFuture = null;
		
		try {
			connectFuture = connector.connect( new InetSocketAddress(host, port) );

			connectFuture.awaitUninterruptibly(this.timeout);

			session = connectFuture.getSession();
			logger.info("connect session : " + session);

		} catch (RuntimeIoException rie) {
			logger.error(rie.getMessage(), rie);
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	public void disconnect() {
		if (session != null) {
			session.closeNow().awaitUninterruptibly(timeout);
//			session.close(true).awaitUninterruptibly();
			session = null;
		}
	}

	public void initData(BusMessage message) {
		logger.info("send message : " + message);
		if (message != null) {
			session.write(message);
		}
	}

	public void createDateFile(BusMessage response) {
		if (response.getCommand().equals(System.getProperty("cmd_hbc"))) {
			SimpleDateFormat dateFormat = new SimpleDateFormat(System.getProperty("date_format"));
			Date time = new Date();
			String nowDate = dateFormat.format(time);

			try (FileWriter fw = new FileWriter(System.getProperty("startDate_path"))) {
//			try (FileWriter fw = new FileWriter("/home/tacs/user/KimJW/CMS/startDate.txt")) {
				fw.write(nowDate);
				logger.info("Date File Write");
			} catch (IOException e) {
				logger.error(e.getMessage(), e);

			}
		}
	}

	@Override
	public void sessionOpened(IoSession session) throws Exception {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		List<String> arrayList = new ArrayList<String>();
		logger.info("session isConnected : " + session.isConnected());
		// Do nothing
		// session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE,
		// SFProtocolClientSessionHandler.defaultIdleTime);
		logger.info("sessionOpened session" + session);
		logger.info("Session Opened!!!!");

		arrayList.add(System.getProperty("source_group"));
		arrayList.add(System.getProperty("source_key"));

		logger.info("SOURCE : " + arrayList);
		hashMap.put("sourceSessionKey", arrayList);
		JSONObject register = new JSONObject(hashMap);
		logger.info("GROUP KEY,VAL : " + register);
		BusMessage message = new BusMessage();

		message.setCommand("REG");
		message.setLength(register.toString().length());
		message.setmValue(register.toString());
		logger.info("REG message : " + message);
		// session.write(message.toString());
		session.write(message);
//		this.requestForResponse(message);
	}

	@Override
	public void sessionClosed(IoSession session) throws Exception {
		// Do nothing
		CMSClientSessionHandler.logger.info("session closed!!");
		this.disconnect();
		
		while (this.isConnected() == false) {
			try {
				Thread.sleep(1000);
				this.connect();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	@Override
	public void messageReceived(IoSession session, Object message) throws Exception {
		BusMessage response = (BusMessage) message;
		logger.info("cmd : " + response.getCommand() + ", msg : " + response.getmValue());
		
		createDateFile(response);
		
		CommandHandlerFactory cf = new CommandHandlerFactory();
		CommandHandler handler = cf.createCommandHandler(this.configHolder, response, session);
		if (handler != null) {
			this.commandHandlerQ.put(handler);
		}
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) {
		// disconnect an idle client
		// session.close(true);
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
		logger.info(session);
		logger.error(cause.getMessage(), cause);
		// this.listener.onException(new Throwable("not connected"));
	}
}