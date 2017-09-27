package edu.usc.polygraph.utils;
import java.io.BufferedInputStream;

import java.io.BufferedOutputStream;

import java.io.BufferedReader;

import java.io.DataInputStream;

import java.io.DataOutputStream;

import java.io.File;

import java.io.FileInputStream;

import java.io.FileNotFoundException;


import java.io.IOException;

import java.io.InputStream;

import java.io.InputStreamReader;

import java.io.OutputStream;

import java.io.PrintWriter;


import java.net.Socket;

import java.net.UnknownHostException;
import java.util.ArrayList;

import java.util.List;

import java.util.Vector;





public class CommonUtilities {

	public static final String M_RESTORE_CHECK_CMD = "mresc";

	public static final String M_RESTORE_CMD = "mrest";

	public static final String M_DELETE_CMD = "mdele";

	public static final String M_DELETE_CHECK_CMD = "mdelc";

	public static int numAttepmts = 10;

	static String os = "windows";

	static String folderNotAvailableMsg = "DIR DOES NOT EXIST";

	static String copyingFailedMsg = "COPYING FAILED";

	static final String NORMAL_CMD = "norml";

	static final String FOLDER_AVAILABILITY_CMD = "check";

	static final String FOLDER_COPY_CMD = "xcopy";

	static final String MYSQL_RUN_CMD = "mysql";

	static final String ORACLE_RUN_CMD = "orcle";

	static final String RESTORE_DB_CMD = "restr";

	static final String RESTORE2_DB_CMD = "rstrw";

	static String dbipOracle = "10.0.0.65";

	static String dbipMySQL = "10.0.0.245";

	static int LPort = 11111;

	public static boolean runningFromLinux=true;

	public static String sshNoCeck=" -o StrictHostKeyChecking=no ";

	public static String cygwinRemoteSSHCmdFormat="C:/cygwin64/bin/ssh " +sshNoCeck+ "  %s \" %s \"";

	public static String linuxRemoteSSHCmdFormat="ssh "+sshNoCeck+" %s  %s ";

	public static boolean antiCacheInit=false;
	/////////////////////
	

		public enum DBState {
			Running, Stopped, Other;
		}








		
		
		
		


		
		
		
		public static void executeRuntime(String cmd, boolean wait) {
			Process p;
			try {
				p = Runtime.getRuntime().exec(cmd);
				if (wait) {
					InputStream stdout = p.getInputStream();
					BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
					String line = "";
					while ((line = reader.readLine()) != null) {
						System.out.println("Commandout: " + line);
					}
					p.waitFor();
				} else
					Thread.sleep(5000);
			} catch (Exception e2) {
				e2.printStackTrace(System.out);
			}

		}

	    public static PrintWriter createBashFilePrintWriter(String FileName) {
			PrintWriter printWriter = null;
			try {

				File file = new File(FileName);
				file.createNewFile();
				printWriter = new PrintWriter(FileName);
				printWriter.println("#!/bin/bash\n\n");
			} catch (FileNotFoundException e) {
				e.printStackTrace(System.out);
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			return printWriter;
		}

		public static void copyFromLinuxRemoteToLocal(String IP, String User, String src, String dist) {
	        PrintWriter printWriter = createBashFilePrintWriter(UtilConstants.CommandFile);

	        String cmd = " scp -r "+IP +":"+ src  + " " +dist + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + UtilConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(UtilConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + UtilConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + UtilConstants.SSHCommandFile, true);
	        executeRuntime(UtilConstants.CommandFile, true);
	    }



	    public static void CreateDir(String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(UtilConstants.CommandFile);

			String cmd = " mkdir -p " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + UtilConstants.CommandFile, true);
			executeRuntime(UtilConstants.CommandFile, true);
		}

		public static void CreateRemoteDir(String IP, String User, String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(UtilConstants.CommandFile);

			String cmd = " sudo mkdir -p " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on ["+IP+"]: " + cmd);
			//System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + UtilConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(UtilConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + UtilConstants.CommandFile;
			printWriter.println(cmd + "\n");
			//System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

			printWriter.close();
			
			executeRuntime("chmod +x " + UtilConstants.SSHCommandFile, true);
			executeRuntime(UtilConstants.SSHCommandFile, true);
			//executeRuntime(CommandFile, true);
		}
		
		public static String fixPath(String path) {
			path = path.replaceAll("/{2,}", "/");
			return path;
		}
		

		public static String fixDir(String dist) {
			if (dist.charAt(dist.length() - 1) != '/')
				dist = dist + "/";
			dist = dist.replaceAll("/{2,}", "/");
			return dist;
		}
		
		public static void deleteRemoteDir(String IP, String User, String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(UtilConstants.CommandFile);

			String cmd = " sudo rm -rf " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on ["+IP+"]: " + cmd);
			//System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + UtilConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(UtilConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + UtilConstants.CommandFile;
			printWriter.println(cmd + "\n");
			//System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);
			printWriter.close();

			executeRuntime("chmod +x " + UtilConstants.SSHCommandFile, true);
			executeRuntime(UtilConstants.SSHCommandFile, true);
			
		
	    
		}    
		public static List<String> executeRuntimeReturn(String cmd, boolean wait) {
			Process p;
			List<String> outs = new ArrayList<String>();
			try {
				p = Runtime.getRuntime().exec(cmd);
				if (wait) {
					InputStream stdout = p.getInputStream();
					BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
					String line = "";
					while ((line = reader.readLine()) != null) {
						outs.add(line);
						System.out.println("Commandout: " + line);
					}
					p.waitFor();
				} else
					Thread.sleep(2000);
			} catch (IOException | InterruptedException e2) {
				e2.printStackTrace(System.out);
			}
			return outs;

		}


	public static void main(String[] args) {}












	 


	  		  
	    public enum Operation {

		Check("Check"),



		BackupOracle("BackupOracle"), RestoreOracle("RestoreOracle"),



		BackupMySQL("BackupMySQL"), RestoreMySQL("RestoreMySQL"),



		startOracle("startOracle"), shutdownOracle("shutdownOracle"),



		startMySQL("startMySQL"), shutdownMySQL("shutdownMySQL"),



		StartCache("StartCache"), StopCache("StopCache"),



		StartCacheStats("StartCacheStats"), StopCacheStats("StopCacheStats"),



		CopyDBStats("CopyDBStats"), Graphes("Graphes"), GraphesWithCache("GraphesWithCache"),



		restoreTheWholeDB("restoreTheWholeDB"), NewMySQLRestore("NewMySQLRestore"), ERROR("ERROR"), CopyCOErrors(

				"CopyCOErrors"), CalculateResults("CalculateResults"), AverageThroughputGraph("AverageThroughputGraph"),



		CountNewOrder("CountNewOrder"), AverageThroughput("AverageThroughput"), DeleteMySQLOldData(

				"DeleteMySQLOldData"), RestoreMySQLData("RestoreMySQLData");



		public final String text;



		Operation(String t) {

			text = t;

		}



		public static Operation getOperation(String OpText) {

			for (Operation op : Operation.values()) {

				if (op.equals(restoreTheWholeDB))

					continue;

				if (op.text.equals(OpText))

					return op;

			}

			return Operation.ERROR;

		}

	}









	public static Vector<StringBuilder> createFiles(String path, String fileNamePattern) {

		Vector<StringBuilder> sv = new Vector<StringBuilder>();

		// Vector<File> filesVector=new Vector<File>();

		File srcFolder = new File(path);

		String files[] = srcFolder.list();

		for (String file : files) {

			File srcFile = new File(srcFolder, file);

			if (!srcFile.isDirectory() && srcFile.getName().contains(fileNamePattern)) {

				// filesVector.add(srcFile);

				StringBuilder s = new StringBuilder();

				readFile(srcFile, s);

				sv.add(s);

			}

		}

		return sv;

	}



	private static void readFile(File srcFile, StringBuilder s) {

		// TODO Auto-generated method stub



		FileInputStream fis = null;

		try {

			fis = new FileInputStream(srcFile);

		} catch (FileNotFoundException e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}



		// Construct BufferedReader from InputStreamReader

		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String newline = System.getProperty("line.separator");

		String line = null;

		try {

			while ((line = br.readLine()) != null) {

				if (line.contains("OS="))

					s.append(line + newline);



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

		// System.out.println("done");

	}











	

	

	public static void executeRuntime(String[] cmd, boolean executeAndResturn){

		Process p;

		try {

			//"C:/cygwin64/bin/ssh mr1@10.0.0.150 \"sar -u 10 > cacheosstats/cpu.txt & \""

			p = Runtime.getRuntime().exec(cmd);

			//_schemaProcess = Runtime.getRuntime().exec("C:/cygwin64/bin/ssh mr1@10.0.0.150 \"killall sar\"");

			if (!executeAndResturn){

			InputStream stdout = p.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));

			String line = "";

			while ((line = reader.readLine()) != null) {

				System.out.println("Commandout: " + line);

			}

			p.waitFor();

			}

			if (executeAndResturn){

				new PrintThread(p).start();

				

					

				

			}

		} catch (IOException | InterruptedException e2) {

			e2.printStackTrace(System.out);

		}

		

		

	}



	

	public static void stopLinuxstats(String dist,String ip, String user, String statsLoc){

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";

//		executeRuntime(cmd,false);

		String cmd="killall sar";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

		if (!runningFromLinux){

		String pass="Yy567999";

		if (user.equals("mr1")){

			pass="Aa123456";

		}

		cmd="pscp -r -pw "+ pass+"  "+user+"@"+ip+":"+statsLoc+"/*.txt "+ dist;//  ;

		executeRuntime(cmd,false);

		}

		else

		{

			 	cmd=" scp "+sshNoCeck+" -r "+ip+":"+ statsLoc+"/*.txt "+ dist;

			 	

				executeRuntime(new String[]{"/bin/sh","-c",cmd},false);

		}

		

//		cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \" cd cacheosstats && rm * \"";

//		executeRuntime(cmd,false);

		 cmd="cd "+statsLoc+" && rm * ";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

	}

	public static boolean startLinuxstats(String ip, String namePattern,String user, String location){

//		

//		String cpuCommand= " sar -u 10 | awk '{ if ($9 ~ /[0-9]/) {sum=100-$9; print sum  }}'";

//		String netCommand="  sar -n DEV 10 | awk '/bond0/{ sum= ($6+$7)/1024; print sum}' ";

//		String memCommand=" sar -r 10 | awk '{ if ($3 ~ /[0-9]/) {sum=$3/1024; print sum}}'";

//		String diskCommand="sar -d 10 | awk '/dev8/{ print $8}'";

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

		String cpuCommand= " sar -P ALL 10 ";

		String netCommand="  sar -n DEV 10  ";

		String memCommand=" sar -r 10 ";

		String diskCommand="sar -d 10 ";

//		String cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \"killall sar \"";

//

//		executeRuntime(cmd,false);

		String cmd="killall sar";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		 cmd="mkdir -p "+ location;

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \""+cpuCommand+" > cacheosstats/1"+namePattern+"cpu.txt & \"";

//		executeRuntime(cmd,false);

		 cmd=cpuCommand+" > "+location+"/1"+namePattern+"cpu.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+netCommand+" > cacheosstats/1"+namePattern+"net.txt & \"";

//		executeRuntime(cmd,false);

		cmd=netCommand+" > "+location+"/1"+namePattern+"net.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);



//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+diskCommand+" > cacheosstats/1"+namePattern+"disk.txt & \"";

//		executeRuntime(cmd,false);

		

		cmd=diskCommand+" > "+location+"/1"+namePattern+"disk.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+memCommand+" > cacheosstats/1"+namePattern+"mem.txt & \"";

//		executeRuntime(cmd,false);

		cmd=memCommand+" > "+location+"/1"+namePattern+"mem.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

		return true;

		

	}
	

	public static String sendMessage(Socket requestSocket, String message, String opType) throws IOException {

		OutputStream out = requestSocket.getOutputStream();

		DataOutputStream os = new DataOutputStream(new BufferedOutputStream(out));

		os.writeBytes(opType + " ");

		os.writeInt(message.length());

		os.writeBytes(message);

		os.flush();



		DataInputStream is = new DataInputStream(new BufferedInputStream(requestSocket.getInputStream()));

		String line = "";

		String response = "0";

		BufferedReader bri = new BufferedReader(new InputStreamReader(is));

		if (opType.equals(FOLDER_AVAILABILITY_CMD)) {

			while ((line = bri.readLine()) != null) {



				if (line.equals(folderNotAvailableMsg)) {

					response = folderNotAvailableMsg;

					System.out.println("Folder doesn't exist!");

				}

			}

		} else if (opType.equals(FOLDER_COPY_CMD)) {

			while ((line = bri.readLine()) != null) {

				if (line.equals("false")) {

					response = copyingFailedMsg;

					System.out.println("copying folder failed!");

				}

			}

		} else if (opType.equals(MYSQL_RUN_CMD)) {

			response = "";

			while ((line = bri.readLine()) != null) {

				response += line;

			}

		} else if (opType.equals(ORACLE_RUN_CMD)) {

			response = "";

			while ((line = bri.readLine()) != null) {

				response = line;

			}

		} 

		

		else if( opType.equals("memch")){

			response=Integer.toString(is.readInt());

		}

		else { // just wait for the output

			while ((line = bri.readLine()) != null)

				response = line;

		}

		/*

		 * int response = is.readInt(); System.out.println(response);

		 * if(response == 0) { // Error }

		 */

		is.close();

		os.close();

		out.close();

		requestSocket.close();

		return response;

	}



	public static boolean isORACLEServerRunning(String ip, String port, int listenerPort) {

		Socket requestSocket;

		// String cmdMsg = "sqlplus benchmark/111111@localhost:" + port +

		// "/ORCL < exitsqlplus.txt";

		String cmdMsg = "sqlplus /nolog < checkOracle.txt";

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, ORACLE_RUN_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}



	/*public static boolean isMySQLServerRunning(String ip, String port, int listenerPort) {

		Socket requestSocket;

		// String cmdMsg = "sqlplus benchmark/111111@localhost:" + port +

		// "/ORCL < exitsqlplus.txt";

		String cmdMsg = "mysqladmin -u cosar --password=gocosar status";

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, MYSQL_RUN_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}*/

	

	public static boolean isMYSQLServerRunning(String ip, String path, int listenerPort) {

		Socket requestSocket;

		//String cmdMsg = "\"" + path + "\\mysqladmin\" ping -u root -p111111";

		String cmdMsg = "C:/mysql/bin/mysqladmin ping -u cosar --password=gocosar";

		try {

			requestSocket = new Socket(ip, listenerPort);

			String output = sendMessage(requestSocket, cmdMsg, MYSQL_RUN_CMD);

			if (output.contains("mysqld is alive"))

				return true;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}

}










class PrintThread extends Thread{

//private static final String SSHCommandFile = null;
//
//private static final String CommandFile = null;

Process p;

String command;

	public PrintThread(Process p1) {

		// TODO Auto-generated constructor stub

		p=p1;

	}

	public PrintThread(String cmd) {

		// TODO Auto-generated constructor stub

		command=cmd;

	}

	public void run(){

		Process p;

		try {

			//"C:/cygwin64/bin/ssh mr1@10.0.0.150 \"sar -u 10 > cacheosstats/cpu.txt & \""

			p = Runtime.getRuntime().exec(command);

			//_schemaProcess = Runtime.getRuntime().exec("C:/cygwin64/bin/ssh mr1@10.0.0.150 \"killall sar\"");

			

			InputStream stdout = p.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));

			String line = "";

			while ((line = reader.readLine()) != null) {

				System.out.println("Commandout: " + line);

			}

			p.waitFor();

			

			

				

					

				

			

		} catch (IOException | InterruptedException e2) {

			e2.printStackTrace(System.out);

		}

	}
	
	
	
	 
	 
	
	

}



