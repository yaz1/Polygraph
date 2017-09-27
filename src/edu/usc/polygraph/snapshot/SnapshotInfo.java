package edu.usc.polygraph.snapshot;

public class SnapshotInfo {
	public long startTime;
	public long endTime;
	public String path;
	public int id;
	
	public SnapshotInfo(){}
	
	public SnapshotInfo(int id, long startTime, long endTime, String path){
		this.id = id;
		this.startTime = startTime;
		this.endTime = endTime;
		this.path = path;
	}
}
