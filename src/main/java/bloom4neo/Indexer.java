package bloom4neo;

import bloom4neo.util.BloomFilter;
import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.IndexGenerator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.procedure.*;

import javax.management.relation.Relation;
import java.util.*;
import java.util.stream.Stream;


public class Indexer {
	//TODO config for all node propertys ... example Lout, Lin , etc. ...
	@Context
	public GraphDatabaseService dbs;
	
	
	/**
	 * Procedure for generating cycle-representative-nodes without 
	 * further reachability index generation <br>
	 * Currently just for Testing
	 */
	@Procedure(value = "generateCylceNodes", mode=Mode.WRITE)
	@Description("Creates Cycle-Representative-Nodes")
	public void procedure_GenerateCycleNodes() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);
	}
	
	
	

	/**
	 * Performs necessary actions to create the reachability index: <br>
	 * 1. Detect Cycles and Create Cycle-Nodes. <br>
	 * TODO ..
	 */

	@Procedure(value = "createIndex", mode=Mode.WRITE)
	@Description("Creating Reachability index for all nodes")
	public void createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);

		// 2. create Index
		IndexGenerator.generateIndex(dbs);

	}

	/**
	 * Returns true if there is an Path between startNode and endNode
	 * @param startNode
	 * @param endNode
	 */
	@Procedure(value = "checkReachability", mode = Mode.READ)
	@Description("Checking Reachability of to nodes")
	public Stream<Reachability> checkReachability(@Name("startNode") Node startNode,
									 @Name("endNode") Node endNode) {

		// 1) same node? return true
		if(startNode.getId() == endNode.getId()){
			Stream<Reachability> result = Stream.of(new Reachability(true));
			return result;
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
			Stream<Reachability> result = Stream.of(new Reachability(true));
			return result;
		}

		//3) filter on first time fail
		if(!checkFilter(startNode, endNode)){
				return Stream.of(new Reachability(false));
		}

		//4) have to check children
		Set<Long> visited = new HashSet<>();
		Queue<Node> adjacentsList = new LinkedList<>();

		for(Relationship r : startNode.getRelationships(Direction.OUTGOING)){
			if(achievedGoal(r.getEndNode(), endNode)){
				return Stream.of(new Reachability(true));
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
					return Stream.of(new Reachability(true));
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

		return Stream.of(new Reachability(false));
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
	public class Reachability {
		public Boolean out;

		public Reachability(Boolean out){
			this.out = out;
		}

		public Boolean getOut(){
			return this.out;
		}

	}


}