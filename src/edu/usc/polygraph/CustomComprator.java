package edu.usc.polygraph;

import java.util.ArrayList;
import java.util.Comparator;

public class CustomComprator implements Comparator<ArrayList<LogRecord>> {

	@Override
	public int compare(ArrayList<LogRecord> o1, ArrayList<LogRecord> o2) {
		return o1.get(o1.size()-1).compareTo(o2.get(o2.size()-1));
	}

}
