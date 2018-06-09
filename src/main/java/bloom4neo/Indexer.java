package bloom4neo;

import bloom4neo.util.BloomFilter;
import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.IndexGenerator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.*;



public class Indexer {
	
	@Context
	public GraphDatabaseService dbs;
	

	/**
	 * Performs necessary actions to create the reachability index: <br>
	 */
	@Procedure(value = "bloom4neo.createIndex_V1", mode=Mode.WRITE)
	@Description("Creating Reachability index for all nodes")
	public void createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		long startTime = System.currentTimeMillis();
		CycleNodesGenerator.generateCycleNodes(dbs);
		System.out.println("Time for GeneratingCycleNodes: " + (System.currentTimeMillis()-startTime));
		
		// 2. create Index
		startTime = System.currentTimeMillis();
		IndexGenerator.generateIndex(dbs);
		System.out.println("Time for Indexing: " + (System.currentTimeMillis()-startTime));
	}
	
	
	/**
	 * Deletes index and generated SCC representatives 
	 */
	@Procedure(name = "bloom4neo.deleteIndex_V1", mode = Mode.WRITE)
	public void procedure_deleteIndex() {
		
		int i = 0;
		for(Node n : dbs.getAllNodes()) {
			
			if(n.hasProperty("cycleRepID")) {
				n.removeProperty("cycleRepID");
			}
			if(n.hasProperty("Lin")) {
				n.removeProperty("Lin");
				i++;
			}
			if(n.hasProperty("Lout")) {
				n.removeProperty("Lout");
			}
		}
		
		System.out.println("Removed " + i + " lin values");
		
		dbs.execute("Match (n:CYCLE_REP) delete (n)");
		
	}

	
	
	/**
	 * Returns true if there is an Path between startNode and endNode
	 * @param startNode
	 * @param endNode
	 */
	@UserFunction(value = "bloom4neo.checkReachability_V1")
	@Description("Checking Reachability of to nodes")
	public boolean checkReachability(@Name("startNode") Object startNodeObj, @Name("endNode") Object endNodeObj) {
		Node startNode = null, endNode = null;
		
		if(startNodeObj instanceof Long && endNodeObj instanceof Long) {
			startNode = dbs.getNodeById((long) startNodeObj);
			endNode = dbs.getNodeById((long) endNodeObj);
		} else if (startNodeObj instanceof Node && endNodeObj instanceof Node) {
			startNode = (Node) startNodeObj;
			endNode = (Node) endNodeObj;
		}
			
		// 1) same node? return true
		if(startNode.getId() == endNode.getId()){
			return true;
		}
		
		// 2) same cycle ? return true
		String cycleIDStart = "";
		String cycleIDEnd = "";
		if (startNode.hasProperty("cycleRepID")) {
			cycleIDStart = startNode.getProperty("cycleRepID").toString();
		}
		if (endNode.hasProperty("cycleRepID")) {
			cycleIDEnd = endNode.getProperty("cycleRepID").toString();
		}

		if(cycleIDStart.equals(cycleIDEnd) && !cycleIDEnd.equals("") && !cycleIDStart.equals("")){
			return true;
		}

		//3) filter on first time fail
		if(!checkFilter(startNode, endNode)){
			return false;
		}

		//4) have to check children
		Set<Long> visited = new HashSet<>();
		Queue<Node> adjacentsList = new LinkedList<>();

		for(Relationship r : startNode.getRelationships(Direction.OUTGOING)){
			if(achievedGoal(r.getEndNode(), endNode)){
				return true;
			}
			if(checkFilter(r.getEndNode(), endNode)){
				adjacentsList.add(r.getEndNode());
			}
		}

		while(adjacentsList.size() > 0){
			Node n = adjacentsList.poll();

			//check children of the child
			for(Relationship r : n.getRelationships(Direction.OUTGOING)){
				if(achievedGoal(r.getEndNode(), endNode)){
					return true;
				}

				long id;
				if(r.getEndNode().hasProperty("cycleRepID")){
					id = dbs.getNodeById(Long.valueOf(r.getEndNode().getProperty("cycleRepID").toString())).getId();
					//todo outgoing cycles relationships
					Node repNode = dbs.getNodeById(Long.valueOf(r.getEndNode().getProperty("cycleRepID").toString()));
					Set<Node> tmp =  CycleNodesGenerator.findNeighbours(repNode);
					for (Node el : CycleNodesGenerator.findNeighbours(repNode)){
						adjacentsList.add(el);
					}
				} else {
					id = r.getEndNodeId();
				}
				if(!visited.contains(id)){
					if(checkFilter(r.getEndNode(), endNode)){
						adjacentsList.add(r.getEndNode());
					}
				}


				visited.add(id);
			}
		}

		return false;
	}
	

	private boolean checkFilter(Node start, Node end){
		BloomFilter filter = new BloomFilter();

		if(start.hasProperty("cycleRepID")){
			start = dbs.getNodeById(Long.valueOf(start.getProperty("cycleRepID").toString()));
		}

		if(end.hasProperty("cycleRepID")){
			end = dbs.getNodeById(Long.valueOf(end.getProperty("cycleRepID").toString()));
		}

		Long startId = start.getId();
		Long endId = end.getId();

		String filterValue = "";

		//checking L_in
		if(end.hasProperty("Lin")){
			filterValue = end.getProperty("Lin").toString();
			if(!filter.check(startId.toString(), filterValue)){
				return false;
			}
		}

		//checking L_out
		if(start.hasProperty("Lout")){
			filterValue = start.getProperty("Lout").toString();
			if(!filter.check(endId.toString(), filterValue)){
				return false;
			}
		}

		return true;
	}
	

	private boolean achievedGoal(Node n, Node endNode){
		//todo achieved goal cycle return also true
		if(n.getId() == endNode.getId()){
			return true;
		} else {
			return false;
		}

	}
	
	//neo4j need a class, returns of procedures have to be streams
	//can have -> also pass into reachability the path to the node (?)
//	public class Reachability {
//		public Boolean out;
//
//		public Reachability(Boolean out){
//			this.out = out;
//		}
//
//		public Boolean getOut(){
//			return this.out;
//		}
//
//	}


}