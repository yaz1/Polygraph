package edu.usc.polygraph.codegenerator;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;

public class VisitedNodes <T> implements DFSVisitor<T> {
	
	HashSet<String> nodes;
	VisitedNodes(){
		nodes=new HashSet<String>();
	}
	@Override
	public void visit(Graph<T> g, Vertex<T> v) {
		// TODO Auto-generated method stub
		nodes.add(v.name);
	}

	@Override
	public void visit(Graph<T> g, Vertex<T> v, Edge<T> e,HashSet<String> allowedEdges) {
	//	if(allowedEdges.contains(e.name))
		nodes.add(v.name);
		
	}

}