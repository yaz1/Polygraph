package edu.usc.polygraph.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class Common {
	public static String ClientOS = "LINUX";
	public static String ServerOS = "LINUX";
	public static String CommandFile = "/home/mr1/CommandFile.sh";
	public static String SSHCommandFile = "/home/mr1/SSHCommandFile.sh";
	public static String DrawWithPythonFile;
	public static String DrawWithPythonAverageThroughputFile;
	public static String DrawWithPythonAverageLatencyFile;
	public static String ResultPath;
	public static int sleepTime = 5000;
	public static String toReplace = "######";
	public static String ClientIP;
	public static String ClientUser;
	public static String ClientStatsPath;
	public static String ClientTemplate = "Client";
	public static int warehouses;

	public static String cmdMoveTo = " cd " + toReplace + " ";
	public static String cmdCPUStats = " sar -P ALL 10 > " + toReplace + "1" + toReplace + "cpu.txt & ";
	public static String cmdNetStats = " sar -n DEV 10 > " + toReplace + "1" + toReplace + "net.txt & ";
	public static String cmdMemStats = " sar -r 10 > " + toReplace + "1" + toReplace + "mem.txt & ";
	public static String cmdDiskStats = " sar -d 10 > " + toReplace + "1" + toReplace + "disk.txt & ";
	public static String cmdIOStats = " iostat -x 10 > " + toReplace + "1" + toReplace + "io.txt & ";

	public static String cmdMakeDir = " mkdir -p " + toReplace + " ";
	public static String cmdDeleteDir = " sudo rm -rf " + toReplace + " ";
	public static String cmdStatsCopy = " scp -r " + toReplace + "*.txt  " + toReplace + " ";
	public static String cmdKillIOStat = " killall iostat ";
	public static String cmdKillSar = " killall sar ";
	public static String cmdKillJava = " killall -9 java ";
	public static String cmdRemoteCopy = " scp " + toReplace + " " + toReplace + " " + toReplace + " ";
	public static String cmdLocalCopy = " sudo cp " + toReplace + " " + toReplace + " " + toReplace + " ";

	public static String cmdDrawWithPython = "python " + toReplace + " " + toReplace + " " + toReplace;

	public static String strRunningCommand(String IP, String cmd) {
		return getTime() + " - Running command on [" + IP + "]: " + cmd;
	}

	public static String strRunningCommands(String IP) {
		return getTime() + " - Running the following commands on [" + IP + "]:-";
	}

	public static void remoteCommand(String IP, String User, boolean wait, String... cmds) {
		PrintWriter printWriter = createBashFilePrintWriter(CommandFile);

		if (cmds.length == 1) {
			printWriter.println(cmds[0] + "\n");
			System.out.println(strRunningCommand(IP, cmds[0]));
		} else {
			System.out.println(strRunningCommands(IP));
			for (int i = 0; i < cmds.length; i++) {
				System.out.println("\t" + cmds[i]);
				printWriter.println(cmds[i] + "\n");
			}
		}

		printWriter.close();

		executeRuntime("chmod +x " + CommandFile, true);

		printWriter = createBashFilePrintWriter(SSHCommandFile);
		String cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + CommandFile;
		printWriter.println(cmd + "\n");

		printWriter.close();

		executeRuntime("chmod +x " + SSHCommandFile, true);
		executeRuntime(SSHCommandFile, wait);
	}

	public static void localCommand(boolean wait, String... cmds) {
		PrintWriter printWriter = createBashFilePrintWriter(CommandFile);

		if (cmds.length == 1) {
			printWriter.println(cmds[0] + "\n");
			System.out.println(strRunningCommand("Local Server", cmds[0]));
		} else {
			System.out.println(strRunningCommands("Local Server"));
			for (int i = 0; i < cmds.length; i++) {
				System.out.println("\t" + cmds[i]);
				printWriter.println(cmds[i] + "\n");
			}
		}

		printWriter.close();

		executeRuntime("chmod +x " + CommandFile, true);
		executeRuntime(CommandFile, wait);
	}

	public static List<String> remoteCommandReturn(String IP, String User, boolean wait, String... cmds) {
		PrintWriter printWriter = createBashFilePrintWriter(CommandFile);

		if (cmds.length == 1) {
			printWriter.println(cmds[0] + "\n");
			System.out.println(strRunningCommand(IP, cmds[0]));
		} else {
			System.out.println(strRunningCommands(IP));
			for (int i = 0; i < cmds.length; i++) {
				System.out.println("\t" + cmds[i]);
				printWriter.println(cmds[i] + "\n");
			}
		}

		printWriter.close();

		executeRuntime("chmod +x " + CommandFile, true);

		printWriter = createBashFilePrintWriter(SSHCommandFile);
		String cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + CommandFile;
		printWriter.println(cmd + "\n");

		printWriter.close();

		executeRuntime("chmod +x " + SSHCommandFile, true);
		return executeRuntimeReturn(SSHCommandFile, wait);

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
				Thread.sleep(sleepTime);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(System.out);
		}

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
				Thread.sleep(sleepTime);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace(System.out);
		}
		return outs;

	}

	public static void CreateDir(String dist) {
		String cmd = variableString(cmdMakeDir, dist);
		localCommand(true, cmd);
	}

	public static void CreateRemoteDir(String IP, String User, String dist) {
		String cmd = variableString(cmdMakeDir, dist);
		remoteCommand(IP, User, true, cmd);
	}

	public static void deleteRemoteDir(String IP, String User, String dist) {
		String cmd = variableString(cmdDeleteDir, dist);
		remoteCommand(IP, User, true, cmd);
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

	public static void startLinuxstats(String IP, String User, boolean local, String Path, String temp) {
		stopLinuxstats(IP, User, local);

		if (local)
			CreateDir(Path);
		else
			CreateRemoteDir(IP, User, Path);

		String cmdCPU = variableString(cmdCPUStats, Path, temp);
		String cmdNet = variableString(cmdNetStats, Path, temp);
		String cmdMem = variableString(cmdMemStats, Path, temp);
		String cmdDisk = variableString(cmdDiskStats, Path, temp);
		String cmdIO = variableString(cmdIOStats, Path, temp);

		if (local)
			localCommand(true, cmdCPU, cmdNet, cmdMem, cmdDisk, cmdIO);
		else
			remoteCommand(IP, User, true, cmdCPU, cmdNet, cmdMem, cmdDisk, cmdIO);
	}

	public static void copyLinuxstats(String IP, String User, boolean local, String src, String Dist) {
		if (local)
			CreateDir(Dist);
		else
			CreateRemoteDir(IP, User, Dist);

		String cmd = variableString(cmdStatsCopy, src, Dist);

		if (local)
			localCommand(true, cmd);
		else
			remoteCommand(IP, User, true, cmd);
	}

	public static void stopLinuxstats(String IP, String User, boolean local) {
		if (local)
			localCommand(true, cmdKillSar, cmdKillIOStat);
		else
			remoteCommand(IP, User, true, cmdKillSar, cmdKillIOStat);
	}

	public static String variableString(String org, String... params) {
		String result = new String(org);
		for (int i = 0; i < params.length; i++) {
			result = result.replaceFirst(toReplace, params[i]);
		}
		return result;
	}

	public static String getTime() {
		Date now = new Date();
		SimpleDateFormat DateTimeFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		return DateTimeFormat.format(now);
	}

	public static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
	}

	public static void createCharts(String filespath, String delimeter) {
		filespath = fixDir(filespath);
		String cmd = variableString(cmdDrawWithPython, DrawWithPythonFile, filespath, delimeter);
		localCommand(true, cmd);
	}

	public enum RemoteCopyType {
		LocalToRemote, RemoteToLocal, RemoteToRemote;
	}

	public static void copyLocalFile(String fromPath, String toPath, boolean Recursive) {
		String rec = "";
		if (Recursive)
			rec = "-r";
		String cmd = variableString(cmdLocalCopy, rec, fromPath, toPath);
		localCommand(true, cmd);
	}

	public static void copyRemoteFile(String fromIP, String fromUser, String fromPath, String toIP, String toUser, String toPath, RemoteCopyType type, boolean Recursive) {
		String src = null, dist = null, rec = "";
		switch (type) {
		case LocalToRemote:
			src = fromPath;
			dist = toUser + "@" + toIP + ":" + toPath;
			break;
		case RemoteToLocal:
			src = fromUser + "@" + fromIP + ":" + fromPath;
			dist = toPath;
			break;
		case RemoteToRemote:
			src = fromUser + "@" + fromIP + ":" + fromPath;
			dist = toUser + "@" + toIP + ":" + toPath;
			break;
		default:
			return;
		}
		if (Recursive)
			rec = "-r";
		String cmd = variableString(cmdRemoteCopy, rec, src, dist);
		localCommand(true, cmd);
	}

	public static void CreateAverageThroughputGraph(String filePath, String fileName) {
		String cmd = variableString(cmdDrawWithPython, DrawWithPythonAverageThroughputFile, filePath, fileName);
		localCommand(true, cmd);
	}

	public static String concat(char seperator, String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
			if (i + 1 != params.length)
				sb.append(seperator);
		}
		return sb.toString();
	}

	public static void createExpConfigFile(String Path) {
		Path = fixDir(Path);
		String configFile = Path + "expConfig.txt";
		PrintWriter printWriter = null;
		try {
			File file = new File(configFile);
			file.createNewFile();
			printWriter = new PrintWriter(configFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			System.exit(1);
		}
		String Line = "################################################";
		printWriter.println(Line);
		printWriter.println("##### Common Settings #####");
		printWriter.println(Line);
		printWriter.println("CommandFile=/home/mr1/rating/temp/CommandFile.sh");
		printWriter.println("SSHCommandFile=/home/mr1/rating/temp/SSHCommandFile.sh");
		printWriter.println("Database=MySQL|Anticaching");
		printWriter.println("NumOfServers=1");
		printWriter.println("ClientOS=LINUX");
		printWriter.println("ServerOS=LINUX");
		printWriter.println("Warehouses=20|50|100");
		printWriter.println("ResultPath=" + Path);
		printWriter.println("ClientIP=10.0.0.119");
		printWriter.println("ServerIP=10.0.0.145");
		printWriter.println("CacheIP=10.0.0.175");
		printWriter.println("ClientUser=mr1");
		printWriter.println("ServerUser=mr1");
		printWriter.println("CacheUser=mr1");
		printWriter.println("CachePort=11211");
		printWriter.println("ClientTemplate=Client");
		printWriter.println("ServerTemplate=Database");
		printWriter.println("CacheTemplate=Cache");
		printWriter.println("ClientStatsPath=/home/mr1/rating/ClientStats/");
		printWriter.println("ServerStatsPath=/home/mr1/ServerStats/");
		printWriter.println("CacheStatsPath=/home/mr1/CacheStats/");
		printWriter.println("ClimbingFactor=2");
		printWriter.println("DrawWithPythonFile=/home/mr1/rating/pythonFiles/admCntrl.py");
		printWriter.println("DrawWithPythonAverageThroughputFile=/home/mr1/rating/pythonFiles/ATG.py");
		printWriter.println("DrawWithPythonAverageLatencyFile=/home/mr1/rating/pythonFiles/ATG.py");

		printWriter.println(Line);
		printWriter.println("##### HsC Settings #####");
		printWriter.println(Line);
		printWriter.println("Host_Side_Cache=false");
		printWriter.println("HsCType=Flashcache|Bcache");
		printWriter.println("HsCWritePolicy=thru|back|around");
		printWriter.println("cachePartition=/dev/sdb1");
		printWriter.println("diskPartition=/dev/sda7");
		printWriter.println("virtual_cache_dev_name=cachedev");
		printWriter.println("virtual_cache_dev_dir=/dev/mapper/cachedev");
		printWriter.println("block_size=4k");

		printWriter.println(Line);
		printWriter.println("##### Twemcache Settings #####");
		printWriter.println(Line);
		printWriter.println("useCache=false");
		printWriter.println("useIQ=false");
		printWriter.println("COType=NotSet|Invalidate|Refill");
		printWriter.println("TwemcachePath=/home/mr1/Desktop/twemcache/src/twemcache");
		printWriter.println("TwemcacheVariables=-t 8 -c 4096 -m 10000 -g 10 -G 100000");

		printWriter.println(Line);
		printWriter.println("##### OLTPBench Settings #####");
		printWriter.println(Line);
		printWriter.println("MySQLBackupPath=/home/mr1/Backup");
		printWriter.println("MySQLDataPath=/home/mr1/Desktop/flashcachedevice/");
		printWriter.println("#MySQLDataPath=/var/lib/mysql");
		printWriter.println("OLTPPath=/home/mr1/oltpbench");
		printWriter.println("#OLTPPath=/home/mr1/Desktop/oltpyaz/oltpbench");
		printWriter.println("warmupTime=600");
		printWriter.println("expTime=600");
		printWriter.println("TPCCWeights=45,43,4,4,4");

		printWriter.println(Line);
		printWriter.println("##### Anticache Settings #####");
		printWriter.println(Line);
		printWriter.println("HStorePathClient=/home/mr1/h-store/");
		printWriter.println("HStorePathServer=/home/mr1/h-store/");
		printWriter.println("AnticacheClientsLogs=/home/mr1/h-store/obj/logs/clients/");
		printWriter.println("AnticacheSitesLogs=/home/mr1/h-store/obj/logs/sites/");
		printWriter.println("AnticacheProperties_File=/home/mr1/h-store/properties/default.properties");
		printWriter.println("AnticacheProperties_ServerFile=/home/mr1/h-store/properties/default.properties");
		printWriter.println("AnticacheProperties_OldFile=/home/mr1/h-store/properties/default.propertiesOLD");
		printWriter.println("AnticacheTPCCProperties_ServerFile=/home/mr1/h-store/properties/benchmarks/tpcc.properties");
		printWriter.println("AnticacheTPCCProperties_File=/home/mr1/h-store/properties/benchmarks/tpcc.properties");
		printWriter.println("AnticacheTPCCProperties_OldFile=/home/mr1/h-store/properties/benchmarks/tpcc.propertiesOLD");
		printWriter.println("Partitions=2");
		printWriter.println("BlocksPerEviction=200");
		printWriter.println("AnticacheThreshold=15000");
		printWriter.println("EditAnticacheProperties=global.defaulthost,10.0.0.119;client.memory,2048;client.txnrate,1000000;client.scalefactor,1;client.duration,300000;client.warmup,300000;site.memory,2048");
		printWriter.println("EditAnticacheTPCCProperties=warehouse_per_partition,false;warehouses,100;neworder_multip,true;neworder_multip_mix,1;payment_multip,true;payment_multip_mix,1");

		printWriter.close();
	}

	public static void printArray(String[] tokens) {
		System.out.print("[");
		for (int i = 0; i < tokens.length; i++) {
			System.out.print(tokens[i]);
			if ((i + 1) != tokens.length)
				System.out.print(", ");
		}
		System.out.println("]");
	}

	public static void addToZipFile(String fileName, String fileLocation, ZipOutputStream zos) throws FileNotFoundException, IOException {

		System.out.println("Writing '" + fileLocation + "' to zip file with the name '" + fileName + "'");

		File file = new File(fileLocation);
		FileInputStream fis = new FileInputStream(file);
		ZipEntry zipEntry = new ZipEntry(fileName);
		zos.putNextEntry(zipEntry);

		byte[] bytes = new byte[1024];
		int length;
		while ((length = fis.read(bytes)) >= 0) {
			zos.write(bytes, 0, length);
		}

		zos.closeEntry();
		fis.close();
	}

	public static void deleteFileOrFolder(final Path path) throws IOException {
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(final Path file, final IOException e) {
				return handleException(e);
			}

			private FileVisitResult handleException(final IOException e) {
				e.printStackTrace(); // replace with more robust error handling
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
				if (e != null)
					return handleException(e);
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	};
}
