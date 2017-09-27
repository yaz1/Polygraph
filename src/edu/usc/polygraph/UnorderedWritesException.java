package edu.usc.polygraph;


public class UnorderedWritesException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 667L;
	public LogRecord record;
	
	public UnorderedWritesException(LogRecord r){
		record = r;
	}
	
	
}


