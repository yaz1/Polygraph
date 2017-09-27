package edu.usc.polygraph.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedHashSet;

public class ReadStaleFile {
	
	public static void main(String[] args) {
		String nodb="/home/mr1/500nodb.txt";
		String db="/home/mr1/eclipsout.txt";
		LinkedHashSet<String> nodbm= readFile(nodb);
		LinkedHashSet<String> dbm= readFile(db);
		System.out.println("nodb size="+nodbm.size());
		System.out.println("db size="+dbm.size());
		System.out.println("Not exist in db:");
		for (String f:nodbm){
			if (!dbm.contains(f)){
				System.out.println(f);
			}
			
		}
		
		
		
		System.out.println("Not exist in nodb:");
		for (String c:dbm){
			if (!nodbm.contains(c)){
				System.out.println(c);
			}
			
		}

		
	}

	private static LinkedHashSet<String> readFile(String file1) {
		LinkedHashSet<String> result=new LinkedHashSet<String>();
		try{
		FileInputStream fis = new FileInputStream(file1);
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.contains("Stale Data")){
				String[] tokens = line.split(":");
				result.add(tokens[1]);
				
			}
		}
	 
		br.close();
		}
		catch(Exception ex){
			ex.printStackTrace(System.out);
		}
		return result;
	}
}
