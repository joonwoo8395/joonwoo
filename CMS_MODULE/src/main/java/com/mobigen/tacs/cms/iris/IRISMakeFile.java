package com.mobigen.tacs.cms.iris;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class IRISMakeFile {
	public final static Logger logger = Logger.getLogger(IRISMakeFile.class);
	
	public String makeCtlFile(String columnStr) {
		String ctl_path = System.getProperty("ctl_file_path");
//		String ctl_path = "/home/tacs/user/KimJW/CMS/loadFiles/cms.ctl";
		columnStr = columnStr.replace(",", "^-^");
		try (FileWriter fw = new FileWriter(new File(ctl_path))){
			fw.write(columnStr);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return ctl_path;
	}
	public String makeDatFile(String originFilePath) {
		String dat_path = System.getProperty("dat_file_path");
//		String dat_path = "/home/tacs/user/KimJW/CMS/loadFiles/cms.dat";

		try (FileWriter fw = new FileWriter(new File(dat_path)); FileReader fr = new FileReader(new File(originFilePath))){
			List<String> lines 			= IOUtils.readLines(fr);
			
			for ( int linesIdx = lines.size() - 1 ; linesIdx < 0;  linesIdx -- ) {
				String[] splitLine = lines.get(linesIdx).split("[|]");		

				System.out.println(splitLine.length);
				if ( splitLine.length != 101 ) {
					lines.remove(linesIdx);
				}
			}
			
			String writeDataStr = String.join("^-^", lines);
			IOUtils.write(writeDataStr, fw);
		
		}catch (IOException e) {
			e.printStackTrace();
		}
		
		return dat_path;
	}
}
