package edu.usc.polygraph.utils;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;



public class ConvertLinuxLogs {

	/**
	 * @param args
	 */
	static boolean firstLine = true;
	static String [] date = null;
	static boolean flag = true;
	static int increment = 0;
	public static final boolean printLinuxHeaders = false;
	public static final String memoryFileStr = "mem";
	public static final String cpuFileStr = "cpu";
	public static final String networkFileStr = "net";
	public static final String diskFileStr = "disk";
	public static final String RESULT_FILE_NAME = "_linuxutil.csv";
	public static final long NETWORK_MAX_BANDWIDTH = 1000000000L;
	
	public static String NETWORK_INTERFACE_NAME = "bond0";
	public static final String DISK_INTERFACE_NAME = "dev8-0";

	static BufferedReader memReader = null;
	static BufferedReader cpuReader = null;
	static BufferedReader netReader = null;
	static BufferedReader diskReader = null;
	static FileWriter fw = null;
	//static BufferedWriter out = null;
	public static void main(String[] args) {
	
	}
	
	public static void ConvertFilesToCSV(String directory, String file_prefix) {
		String CPUFile = directory + "1" + file_prefix + cpuFileStr + ".txt";
		String DiskFile = directory + "1" + file_prefix + diskFileStr + ".txt";
		String MemFile = directory + "1" + file_prefix + memoryFileStr + ".txt";
		String NetFile = directory + "1" + file_prefix + networkFileStr + ".txt";
		ConvertCPUFilesToCSV(CPUFile, file_prefix);
		ConvertMemFilesToCSV(MemFile, file_prefix);
		ConvertDiskFilesToCSV(DiskFile, file_prefix);
		ConvertNetFilesToCSV(NetFile, file_prefix);
	}
	
	
	
	

	private static void ConvertCPUFilesToCSV(String file, String file_prefix) {
		File cpufile = null;
		BufferedWriter outcpu = null;
		boolean gotHeaders = false;

		try {
			cpufile = new File(file);
			cpuReader = new BufferedReader(new FileReader(cpufile));
			String outFile = cpufile.getParent() + "/" + file_prefix + cpuFileStr + ".txt";
			outcpu = new BufferedWriter(new FileWriter(outFile));

			String cpuText = null;

			while ((cpuText = cpuReader.readLine()) != null) {
				while (!cpuText.contains("%idle")) {
					cpuText = cpuReader.readLine();
					if (cpuText == null)
						break;
				}

				if (cpuText != null) {
					String[] cpuLine = null;
					String cpuOutLine = "";
					String cpuHeadersLine = "";
					while (!cpuText.equals("")) {
						cpuText = cpuReader.readLine();
						if (cpuText == null || cpuText.length() == 0)
							break;
						cpuLine = cpuText.split("\\s+");
						double i = Double.parseDouble(cpuLine[8]);
						i = 100 - i;
						if(!gotHeaders)
							cpuHeadersLine += cpuLine[2] + ",";
						cpuOutLine = cpuOutLine + i + ",";
					}
					if(!gotHeaders){
						outcpu.write(cpuHeadersLine + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outcpu.write(cpuOutLine + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (cpuReader != null) {
					cpuReader.close();
				}
				outcpu.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertMemFilesToCSV(String file, String file_prefix) {
		File memfile = null;
		BufferedWriter outmem = null;

		try {
			memfile = new File(file);
			memReader = new BufferedReader(new FileReader(memfile));
			String outFile = memfile.getParent() + "/" + file_prefix + memoryFileStr + ".txt";
			outmem = new BufferedWriter(new FileWriter(outFile));

			String memText = null;
			memReader.readLine();
			memReader.readLine();
			memReader.readLine();

			while ((memText = memReader.readLine()) != null) {
				String[] memLine = memText.split("\\s+");
				double i = (Double.parseDouble(memLine[2]) + Double.parseDouble(memLine[6])) / 1024;
				outmem.write(i + "," + System.getProperty("line.separator"));
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (memReader != null) {
					memReader.close();
				}
				outmem.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertNetFilesToCSV(String file, String file_prefix) {
		File netfile = null;
		BufferedWriter outnet = null;
		boolean gotHeaders = false;

		try {
			netfile = new File(file);
			netReader = new BufferedReader(new FileReader(netfile));
			String outFile = netfile.getParent() + "/" + file_prefix + networkFileStr + ".txt";
			outnet = new BufferedWriter(new FileWriter(outFile));

			String netText = null;

			while ((netText = netReader.readLine()) != null) {
				while (!netText.contains("IFACE")) {
					netText = netReader.readLine();
					if (netText == null)
						break;
				}

				if (netText != null) {
					String[] netLine = null;
					String netOutLine = "";
					String netHeadersLine = "";
					while (!netText.equals("")) {
						netText = netReader.readLine();
						if (netText == null || netText.length() == 0)
							break;
						netLine = netText.split("\\s+");

						double rx = Double.parseDouble(netLine[5]);
						double tx = Double.parseDouble(netLine[6]);
						double i = (rx + tx) / 1024;
						if(!gotHeaders)
							netHeadersLine += netLine[2] + "," + netLine[2] + "_R," + netLine[2] + "_T,";
						netOutLine += i + "," + (rx/1024) + "," + (tx/1024) + ",";
					}
					if(!gotHeaders){
						outnet.write(netHeadersLine + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outnet.write(netOutLine + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (netReader != null) {
					netReader.close();
				}
				outnet.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertDiskFilesToCSV(String file, String file_prefix) {
		File diskfile = null;
		BufferedWriter outdisk = null;
		BufferedWriter outdiskRW = null;
		boolean gotHeaders = false;

		try {
			diskfile = new File(file);
			diskReader = new BufferedReader(new FileReader(diskfile));
			String outFile = diskfile.getParent() + "/" + file_prefix + diskFileStr + ".txt";
			String outRWFile = diskfile.getParent() + "/" + file_prefix + diskFileStr + "RW.txt";
			outdisk = new BufferedWriter(new FileWriter(outFile));
			outdiskRW = new BufferedWriter(new FileWriter(outRWFile));

			String diskText = null;

			while ((diskText = diskReader.readLine()) != null) {
				while (!diskText.contains(DISK_INTERFACE_NAME)) {
					diskText = diskReader.readLine();
					if (diskText == null)
						break;
				}

				if (diskText != null) {
					String[] diskLine = null;
					diskLine = diskText.split("\\s+");
					if(!gotHeaders){
						outdisk.write(diskLine[2] + "," + System.getProperty("line.separator"));
						outdiskRW.write(diskLine[2] + "_R," + diskLine[2] + "_W," + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outdisk.write(diskLine[7] + "," + System.getProperty("line.separator"));
					outdiskRW.write(diskLine[5] + "," + diskLine[6] + "," + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (diskReader != null) {
					diskReader.close();
				}
				outdisk.close();
				outdiskRW.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}



	
	/***
     * Find the full file names for any files in the directory that match the
     * logfile prefixes specified in logfiles.
     * @param directory
     * @param logfiles
     * @return
     */
    public static String[] findActualLogNames(String directory, String[]logfiles)
    {
    	File dir =  new File(directory);
//    	FilenameFilter filter = new FilenameFilter() {
//    		private String prefix;
//    		public boolean accept (File dir, String name) {
//    			return name.startsWith(prefix);
//    		}
//    		
//    		public void setPrefix(String name) {
//    			prefix = name;
//    		}    		
//    	};
    	
    	String files[] = dir.list();;
    	String results[] = new String[logfiles.length];
    	for(int i = 0; i < logfiles.length; i++) {
    		for(int j = 0; j < files.length; j++) {
    			if(files[j].contains(logfiles[i])) {
    				results[i] = files[j];
    				break;
    			}
    		}
    	}
    	
    	for(int i = 0; i < results.length; i++)
    	{
    		if(results[i] == null)
    		{
    			System.out.println("ERROR. No matching file for a specified log file prefix: "+logfiles[i]);
    			return null;
    		}
    	}
    	return results;    	    	
    }

	
}