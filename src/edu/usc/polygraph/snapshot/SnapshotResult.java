package edu.usc.polygraph.snapshot;

import java.util.ArrayList;
import java.util.HashSet;

import edu.usc.polygraph.ValidatorData;

public class SnapshotResult {
	public ValidatorData data;
	public HashSet<Long> unorderedReads;
	public HashSet<Long> unorderedWrites;
	public ArrayList<Long> skippedReads;
	public ArrayList<Long> skippedWrites;
}
