package edu.usc.polygraph.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Vector;

public class CreateResourcesFiles {
	public static Vector<StringBuilder> createFiles(String path, String fileNamePattern){
		Vector<StringBuilder> sv=new Vector<StringBuilder>();
//		Vector<File> filesVector=new Vector<File>();
		File srcFolder = new File(path);
		String files[] = srcFolder.list();
		for (String file : files) {
			File srcFile = new File(srcFolder, file);
			if (!srcFile.isDirectory()&& srcFile.getName().contains(fileNamePattern)){
//				filesVector.add(srcFile);
			StringBuilder s=new StringBuilder();
			readFile(srcFile,s);
			sv.add(s);
			}
		}
	
		return sv;
		
			
			
		
	}

	public static void createResourcesFiles(Vector<StringBuilder> sv,String path,String delimeter) throws Exception {
		
		// TODO Auto-generated method stub
		//int index;
		String[] lines;
		String cpu="",net="",mem="",disk="";
		try {
			
			PrintWriter cpuwriter = new PrintWriter(path+"/"+ delimeter+"cpu.txt", "UTF-8");
			PrintWriter memwriter = new PrintWriter(path+"/"+ delimeter+"mem.txt", "UTF-8");
			PrintWriter netwriter = new PrintWriter(path+"/"+ delimeter+"net.txt", "UTF-8");
			PrintWriter diskwriter = new PrintWriter(path+"/"+ delimeter+"disk.txt", "UTF-8");
			
		
	    String newline=System.getProperty("line.separator");
		String [][]ss= new String[sv.size()][];
		String line="";
		int minl=Integer.MAX_VALUE;
		for(int i=0;i<sv.size();i++){
		ss[i]=sv.get(i).toString().split(newline);
		if(ss[i].length<minl)
			minl=ss[i].length;
		
		}
		sv.clear();
		for (int i=0;i<minl;i++){
			line="";
			for(int j=0;j<ss.length;j++)
			{
				line=line+ss[j][i]+"#";
				
			}
			lines=line.split("#");
			for (String l:lines)
			{
				cpu=cpu+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				mem=mem+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				net=net+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				disk=disk+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				try{
					// for Disk reads
				l=l.substring(l.indexOf(',')+1);
				if (l.contains(":"))
				disk=disk+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				}
				catch (Exception ex){
					System.out.println(ex.getMessage());
				}
				
				try{
					for (int c=0; c< 8;c++)
					{
					l=l.substring(l.indexOf(',')+1);
					if (l.contains(":"))
					cpu=cpu+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
					}
					}
					catch (Exception ex){
						System.out.println(ex.getMessage());
					}
				
				
			}
			cpuwriter.println(cpu);
			netwriter.println(net);
			memwriter.println(mem);
			diskwriter.println(disk);
			cpu="";
			net="";
			mem="";
			disk="";
			
			
			
		}
		cpuwriter.close();
		netwriter.close();
		memwriter.close();
		diskwriter.close();

		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		
	}

	private static void readFile(File srcFile, StringBuilder s) {
		// TODO Auto-generated method stub
		
		FileInputStream fis=null;
		try {
			fis = new FileInputStream(srcFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	    String newline=System.getProperty("line.separator");
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				if (line.contains("OS="))
				s.append(line+newline);
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
//		System.out.println("done");
	}
	
	public static void main(String args[]) {
	}
	
	public static void createCharts(String filespath,String delimeter, String pythonscriptpath) {
		// TODO Auto-generated method stub
	
		String cmd="python "+pythonscriptpath+"admCntrl.py "+filespath+"/ "+delimeter;
		try {
			Process p=Runtime.getRuntime().exec(cmd);
			InputStream out = p.getErrorStream();
			BufferedReader reader = new BufferedReader (new InputStreamReader(out));				
			String line ="";
		//	Thread.sleep(60000);
			//p.destroy();
			while ((line = reader.readLine ()) != null ) 
			{
			
		
			System.out.println ("Pythonout: "+ line);
			}
			p.waitFor();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		
	}

	public static void createThruCharts(String filespath, String pythonscriptpath) {
		
		String cmd="python "+pythonscriptpath+"throughput.py "+filespath + "/";
		try {
			Process p=Runtime.getRuntime().exec(cmd);
			InputStream out = p.getErrorStream ();
			BufferedReader reader = new BufferedReader (new InputStreamReader(out));				
			String line ="";
		//	Thread.sleep(60000);
			//p.destroy();
			while ((line = reader.readLine ()) != null ) 
				{
//				
			
				System.out.println ("Pythonout: "+ line);
				}
			p.waitFor();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		
	}

}