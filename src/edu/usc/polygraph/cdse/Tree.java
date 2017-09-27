package edu.usc.polygraph.cdse;
import java.awt.Container;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import javax.swing.BorderFactory;
import javax.swing.JComponent;
import javax.swing.JDialog;

import org.abego.treelayout.TreeForTreeLayout;
import org.abego.treelayout.TreeLayout;
import org.abego.treelayout.demo.TextInBox;
import org.abego.treelayout.demo.TextInBoxNodeExtentProvider;
import org.abego.treelayout.demo.swing.TextInBoxTreePane;
import org.abego.treelayout.util.DefaultConfiguration;
import org.abego.treelayout.util.DefaultTreeForTreeLayout;

import edu.usc.polygraph.LogRecord;


public class Tree {
  Node root;
  long noNodes=1;
  
  Tree(){
	  
  }
  Tree(Node r){
	  noNodes=1;
	  root=r;
	  
  }
  public void addChildToParent(Node parent, Node child){
	  // parent already in tree
	  parent.addChild(child);
  }
  
  public void setRoot(Node r)
  {
	  root=r;
  }
  
  public Node getRoot()
  {
	  return root;
  }
  public void buildTree(){
//	  Node w1= new Node("w1");
//	  Node w2= new Node("w2");
//	  Node w1_1= new Node("w1_1");
//	  w1.addChild(w1_1);
//	  root.addChild(w2);
//	  root.addChild(w1);


  }
	public static void main(String[] args) {
	
		Tree t1= new Tree();
		t1.buildTree();
		
	}
//	public void mergeSameDataItems(){
//		mergeSame(root);
//	}
//	private void mergeSame(Node n) {
//		Iterator<Node> it = n.children.iterator();
//		while(it.hasNext()){
//			Node child=it.next();
//			if (n.sameDataItems(child)){
//				merge(n,child);
//				break;
//			}
//			mergeSame(child);
//		}
//	
//		
//	}
	private void merge(Node parent, Node child) {
		//1- delete child from parent
		//it.remove();
		parent.children.remove(child);
		parent.addTrans(child.transactions);
		for (Node gchild:child.children){
			parent.addChild(gchild);
			
		}
		child.children.clear();
		
	}
	public ArrayList<Node> depthFirstSearchPost(){
		ArrayList<Node> path=new ArrayList<Node>();
		HashSet <Node> visited= new HashSet<Node>();
		doDFSPost(this.root, visited,path);
		
		return path;
	}
	
	public void findPathsDFS(int size,ArrayList<ArrayList<LogRecord>> paths,ArrayList<LogRecord> input){
		Node[]path=new Node[size];
	
		doPathDFS(this.root,path,0,paths,input);
		
	
	}
	
	private void doPathDFS(Node n, Node[]path, int pathlen, ArrayList<ArrayList<LogRecord>> paths, ArrayList<LogRecord> input) {
		path[pathlen] =n;
		pathlen++;
		//CDSE.pathsCounter++;
		if (n.children.size()==0 ){
			ArrayList<LogRecord> p1 = getPath(path,pathlen,input);
			if(p1!=null)
			paths.add(p1);
			
		}
		//for (int i=n.children.size()-1; i>=0;i--){
			for (int i=0 ;i<n.children.size(); i++){

			
			Node child= n.children.get(i);
			
			doPathDFS(child,path,pathlen,paths,input);
			
			
		}
		
		
	}
	private ArrayList<LogRecord> getPath(Node[] path, int pathlen, ArrayList<LogRecord> input) {
		ArrayList<LogRecord> result= new ArrayList<LogRecord>();
	//	ArrayList<Node> nodes= new ArrayList<Node>();

		Node last=path[pathlen-1];
		//HashSet<String> mytrans= new HashSet<String>();
		if (last.remainDataItems.size()!= 0)
			return null;
		for (int i=pathlen-1;i>=0;i--){
		//	CDSE.pathsCounter++;
			Node n = path[i];
			LogRecord trans = n.transactions.get(0);
			result.add(trans);
//			if (Main.CHECK_TIME){
//				nodes.add(n);
//			mytrans.add(trans.name);
//			}
	
		}
		
		if (CDSE.CHECK_TIME){
//			boolean r=true;
//			if(input.size()!=mytrans.size()){
//			r=getRemaining(nodes, mytrans,input);
//			}
//			if (!r)
//			return null;
		assert CDSE.isValidSchedule(result);
		}
		return result;
	}
//	private boolean getRemaining(ArrayList<Node> nodes, HashSet<String> mytrans, ArrayList<Transaction> input) {
//		ArrayList<Transaction> remain=new ArrayList<Transaction>();
//		for (Transaction tran :input){
//			//Transaction tran = n.transactions.get(0);
//			if (!mytrans.contains(tran.name)){
//				remain.add(tran);
//			}
//			
//		}
//		Collections.sort(remain,Transaction.Comparators.COMMIT_REV);
//		for (Transaction tran: remain){
//			Transaction first=tran.findFirstOverlapping(nodes);
//			if (tran.startTime <= first.commitTime){
//				Node l = nodes.get(nodes.size()-1);
//				if (tran.commitTime <l.getCommit()){
//					l.setCommit(tran.commitTime);
//				}
//			}
//			else{
//				return false;
//			}
//			
//
//		}
//		return true;
//	}
	
	private void printPath(Node[] path, int pathlen) {
		for (int i=0;i< pathlen;i++){
			Node n = path[i];
			if (n!=null)
			System.out.print(n.name+",");
		}
		System.out.println();
		
	}
	public ArrayList<Node> depthFirstSearchInOrder(boolean right){
		ArrayList<Node> path=new ArrayList<Node>();
		HashSet <Node> visited= new HashSet<Node>();
		doDFSInOrder(this.root, visited,path,right);
		
		return path;
	}
	
	public ArrayList<Node> depthFirstSearchInOrderRight(){
		ArrayList<Node> path=new ArrayList<Node>();
		HashSet <Node> visited= new HashSet<Node>();
		doDFSInOrderRight(this.root, visited,path);
		
		return path;
	}
	private void doDFSPost(Node n, HashSet<Node> visited, ArrayList<Node> path) {
		if (n==null || visited.contains(n))
			return;
		visited.add(n);
		//for (int i=n.children.size()-1; i>=0;i--){
			for (int i=0 ;i<n.children.size(); i++){

			
			Node child= n.children.get(i);
			doDFSPost(child,visited,path);
			
		}
		path.add(n);
		
		
	}
	
private void doDFSInOrderRight(Node n, HashSet<Node> visited, ArrayList<Node> path) {
		
		if (n==null || visited.contains(n))
			return;
		visited.add(n);
		//for (int i=n.children.size()-1; i>=0;i--){
			boolean added=false;
		
			for (int i=n.children.size()-1;i>=0; i--){
			
			if (n.children.size()==1){
				if (!added){
					path.add(n);
					added=true;
					}
			}
			Node child= n.children.get(i);
			doDFSInOrderRight(child,visited,path);
			if (!added){
			path.add(n);
			added=true;
			}
		}
			if (!added){
				path.add(n);
				added=true;
				}
	}
	
	private void doDFSInOrder(Node n, HashSet<Node> visited, ArrayList<Node> path, boolean right) {
		
		if (n==null || visited.contains(n))
			return;
		visited.add(n);
		//for (int i=n.children.size()-1; i>=0;i--){
			boolean added=false;
			int addedIndex=0;
			int counter=-1;
			for (int i=0 ;i<n.children.size(); i++){
				if (n.children.size()==1&& right){
					if (!added){
						path.add(n);
						added=true;
						}
				}
				counter++;
			if (right){
				addedIndex=n.children.size()-1-i-counter;
			}
			Node child= n.children.get(i+addedIndex);
			doDFSInOrder(child,visited,path,right);
			if (!added){
			path.add(n);
			added=true;
			}
		}
			if (!added){
				path.add(n);
				added=true;
				}
	}
	public ArrayList<Node> breadthFirstSearch(){
		ArrayList<Node> path=new ArrayList<Node>();
		HashSet <Node> visited= new HashSet<Node>();
		Queue <Node>queue = new LinkedList<Node>();
		queue.add(root);
		visited.add(root);
		while(!queue.isEmpty()){
			Node n = queue.poll();
			visited.add(n);
			path.add(n);
			for (Node child:n.children){
				if (!visited.contains(child)){
					queue.add(child);
				}
				
			}
			
		}
		
		return path;
	}
	
	private static void showInDialog(JComponent panel) {
		JDialog dialog = new JDialog();
		Container contentPane = dialog.getContentPane();
		((JComponent) contentPane).setBorder(BorderFactory.createEmptyBorder(
				10, 10, 10, 10));
		contentPane.add(panel);
		dialog.pack();
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
	}
	public void visualize() {
		TreeForTreeLayout<TextInBox> tree= createVisualTree();
		// setup the tree layout configuration
				double gapBetweenLevels = 50;
				double gapBetweenNodes = 10;
				DefaultConfiguration<TextInBox> configuration = new DefaultConfiguration<TextInBox>(
						gapBetweenLevels, gapBetweenNodes);

				// create the NodeExtentProvider for TextInBox nodes
				TextInBoxNodeExtentProvider nodeExtentProvider = new TextInBoxNodeExtentProvider();

				// create the layout
				TreeLayout<TextInBox> treeLayout = new TreeLayout<TextInBox>(tree,
						nodeExtentProvider, configuration);

				// Create a panel that draws the nodes and edges and show the panel
				TextInBoxTreePane panel = new TextInBoxTreePane(treeLayout);
				showInDialog(panel);

		
	}
	private TreeForTreeLayout<TextInBox> createVisualTree() {
	
		TextInBox troot = new TextInBox(this.getRoot().name, 40, 20);
	

		DefaultTreeForTreeLayout<TextInBox> tree = new DefaultTreeForTreeLayout<TextInBox>(
				troot);
		addChildrenVisualTree(tree, this.root,troot);
	
		return tree;
	}
	private void addChildrenVisualTree(DefaultTreeForTreeLayout<TextInBox> tree, Node n, TextInBox parent) {
		if (n.children.isEmpty())
			return;
		
		for (Node child: n.children){
			TextInBox c1 = new TextInBox(child.name, 40, 20);
			tree.addChild(parent, c1);
			addChildrenVisualTree(tree,child,c1);

		}
		
	}

}
