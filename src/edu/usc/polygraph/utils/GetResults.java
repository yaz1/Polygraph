package edu.usc.polygraph.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class GetResults {

	public static void main(String[] args) {
		String dir="C:\\cygwin64\\home\\yaz\\newResults3\\uniform\\";
		String file="allResults.txt";
		List<String> files = FileSearch.findFiles(dir, file);
		SortedMap<Integer, Stats> results= new TreeMap<Integer, Stats>();
		for (String f: files){
			FileInputStream fis;
			try {
				fis = new FileInputStream(new File(f));
			
			//Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		 
			String line = null;
			while ((line = br.readLine()) != null) {
				
				try{
			//	System.out.println(line);
				String thread= line.split(",")[0];
				
				Stats stat = results.get(Integer.parseInt(thread));
				if (stat==null){
					stat= new Stats();
					results.put(Integer.parseInt(thread), stat);
				}
				updateStats(stat,line);
				}
				catch(Exception ex){
					System.out.println("Exception:"+ ex.getMessage()+" line:"+line+ " file:"+f);
				}
			}
		 
			br.close();
			
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
		}
		
		printResults(results);
		
		

	}

	private static void printResults(SortedMap<Integer, Stats> results) {
		System.out.println("Thread,Average,Max,Min,Avg scale,Max scale,Min scale,Num Exp");
		double max = 0,min = 0,avg = 0;
		boolean flag=false;
		for (int key: results.keySet()){
			Stats stat = results.get(key);
			if(!flag){
				flag=true;
				min=stat.min;
				max=stat.max;
				avg=stat.avg;
			}
			
			System.out.printf("%d,%.0f,%.0f,%.0f,%.2f,%.2f,%.2f,%d \n",key,stat.avg,stat.max,stat.min, avg/stat.avg,max/stat.min,min/stat.max,stat.values.size());
		}
		
	}

	private static void updateStats(Stats stat, String line) {
		String[] tokens = line.split(",");
		if (tokens.length<5)
			return;
		String[] time= tokens[4].split(":");
		double seconds= (Double.parseDouble(time[0])*60);
		seconds+=Double.parseDouble(time[1].substring(0, time[1].indexOf('.')));
		stat.values.add(seconds);
		if (seconds< stat.min)
			stat.min=seconds;
		if (seconds>stat.max)
			stat.max=seconds;
		double sum=0;
		for (double d: stat.values)
		{
			sum+=d;
		}
		stat.avg=sum/stat.values.size();
	}

}

class Stats{
	ArrayList<Double> values= new ArrayList<Double>();
	double max=Double.MIN_VALUE;
	double min=Double.MAX_VALUE;
	double avg=0;
}
