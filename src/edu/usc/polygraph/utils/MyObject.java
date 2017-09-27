package edu.usc.polygraph.utils;
public class MyObject implements Comparable<MyObject> {
	public long ST;
	public String line;

	public MyObject(long ST, String line) {
		this.ST = ST;
		this.line = line;
	}

	@Override
	public int compareTo(MyObject o) {
		return Long.compare(ST, o.ST);
	}
}
