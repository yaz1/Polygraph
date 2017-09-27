package edu.usc.polygraph;

import java.util.HashMap;

import edu.usc.dblab.cm.sizers.ProfileSizer;
import edu.usc.dblab.cm.sizers.ReflectionSizer;
import edu.usc.dblab.cm.sizers.Sizer;

public class TestMemory {
	public static void main(String[] args) {
		Sizer sizer = new ProfileSizer();
		
		HashMap<String, Integer> hm = new HashMap<>();
		hm.put("key1", 5);
		hm.put("key2", 7);
		
		System.out.println(sizer.GetSize(hm));
		
		sizer = new ReflectionSizer();
		System.out.println(sizer.GetSize(hm));
	}
}
