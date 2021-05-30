package com.mobigen.tacs.cms.execute;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class testMain {

	public static void main(String[] args) {
		try (FileWriter fw = new FileWriter(new File("\\\\DESKTOP-L3M3VG0\\Users\\Administrator\\공유 폴더\\KimJw\\test.txt")); FileReader fr = new FileReader(new File("\\\\DESKTOP-L3M3VG0\\Users\\Administrator\\공유 폴더\\KimJw\\cims_cb_user.txt"))){
			List<String> lines = IOUtils.readLines(fr);
			System.out.println(lines.get(0));
			System.out.println(lines.get(1));
			System.out.println(lines.get(2));
			IOUtils.copy(fr, fw);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
