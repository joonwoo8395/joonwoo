package com.mobigen.tacs.cms.iris;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		String filePath 	= "D:\\date";
//		File file 			= new File(filePath);
//		String fileList[] 	= file.list();
		
		DateFormat dateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
		
		Date date = null;
		try {
			date = dateFormat1.parse("20190716141125");
			System.out.println(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Calendar calendar 	= Calendar.getInstance();
		
		calendar.setTime(date);
		calendar.add(Calendar.HOUR, -1);
		System.out.println(date);
		System.out.println(calendar.getTime());
		String createDate	= dateFormat.format(date);
		String noDelDate	= dateFormat.format(calendar.getTime());
		
		System.out.println(createDate);
		System.out.println(noDelDate);
		
		for ( File oneFile : new File(filePath).listFiles() ) {
			String filename = oneFile.getName();
			if ( filename.contains(noDelDate) == false && filename.contains(createDate) == false ) {
				boolean isDeleted = oneFile.delete();
				System.out.println(String.format("deleteFilename:%s, result:%s", filename, isDeleted));
			}
		}
//		HashMap<String,Object> hasMap = new HashMap<String,Object>();
//		ArrayList<String> hashMap = new ArrayList<String>();
//		
//		// Do nothing
//		// session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE,
//		// SFProtocolClientSessionHandler.defaultIdleTime);
//		
//		String[] source = { Define.SOURCE_GROUP, Define.SOURCE_KEY };
//		
//		System.out.println("SOURCE : " + Arrays.deepToString(source));
//		hashMap.add(Define.SOURCE_GROUP);
//		hashMap.add(Define.SOURCE_KEY);
//		hasMap.put("session", hashMap);
//		JSONObject register = new JSONObject(hasMap);
//		System.out.println("GROUP KEY,VAL : " + register);
//		
//		BusMessage message = new BusMessage();
//
//		message.setCommand("REG");
//		message.setLength(register.toString().length());
//		message.setmValue(register.toString());
//		System.out.println("REG message : " + message);
		// session.write(message.toString());
//		this.requestForResponse(message);
//		IRISConnect iris = new IRISConnect();
//		Connection conn = iris.connect();
//
//		System.out.println(conn); 
//		
//		iris.selectData(conn);
//		
//		
//		
//		
		// iris.loadData(conn, Define.TABLE, Define.KEY, Define.PARTITION,
		// Define.CTL_FILE_PATH, Define.DAT_FILE_PATH);

	}
}
