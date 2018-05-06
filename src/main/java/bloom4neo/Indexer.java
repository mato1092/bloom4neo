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

import java.util.ArrayList;
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
		BloomFilter filter = new BloomFilter();

		// 1) same node? return true
		if(startNode.getId() == endNode.getId()){
			Stream<Reachability> result = Stream.of(new Reachability(true));
			return result;
		}
		
		// 2) check for cycles
		String cycleIDStart = null;
		String cycleIDEnd = null;
		if (startNode.hasProperty("cycleRepID")) {
			cycleIDStart = startNode.getProperties("cycleRepID").toString();
		}
		if (endNode.hasProperty("cycleRepID")) {
			cycleIDEnd = endNode.getProperties("cycleRepID").toString();
		}
		
		// 3) if both are in a cycle
		if (cycleIDStart != null && cycleIDEnd != null) {
			// same Cycle -> return true
			if (cycleIDStart.equals(cycleIDEnd)) {
				return Stream.of(new Reachability(true));
			} 
			// different Cycles -> Check BF for CycleIDs
			else {
				if (startNode.hasProperty("Lout") || endNode.hasProperty("Lin")) {
					//TODO throw exception - not indexed
				}
				
				String filterValue;
				
				// endNodeCycle in lout of startNodeCycle?
				filterValue = startNode.getProperty("Lout").toString();
				if(!filter.check(cycleIDEnd, filterValue)){
					Stream<Reachability> result = Stream.of(new Reachability(false));
					return result;
				}	
				// startNodeCylce in lin of endNodeCylce?
				filterValue = endNode.getProperty("Lin").toString();
				if(!filter.check(cycleIDStart, filterValue)){
					Stream<Reachability> result = Stream.of(new Reachability(false));
					return result;
				}
			}	
		}
		
		// 4) if startNode is in a cycle
		if (cycleIDStart != null && cycleIDEnd == null) {
			if (startNode.hasProperty("Lout") || endNode.hasProperty("Lin")) {
				//TODO throw exception - not indexed
			}
			
			String filterValue;
			
			// endNode in lout of startNodeCycle?
			filterValue = startNode.getProperty("Lout").toString();
			if(!filter.check(Long.toString(endNode.getId()), filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}	
			// startNodeCylce in lin of endNode?
			filterValue = endNode.getProperty("Lin").toString();
			if(!filter.check(cycleIDStart, filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}	
		}
		
		// 5) if EndNode is in a cycle
		if (cycleIDStart == null && cycleIDEnd != null) {
			if (startNode.hasProperty("Lout") || endNode.hasProperty("Lin")) {
				//TODO throw exception - not indexed
			}
			
			String filterValue;
			
			// endNodeCycle in lout of startNode?
			filterValue = startNode.getProperty("Lout").toString();
			if(!filter.check(cycleIDEnd, filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}	
			// startNode in lin of endNodeCycle?
			filterValue = endNode.getProperty("Lin").toString();
			if(!filter.check(Long.toString(startNode.getId()), filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}	
		}
		
		// 6) no cycles
		if (cycleIDStart == null && cycleIDEnd == null) {
			if (startNode.hasProperty("Lout") || endNode.hasProperty("Lin")) {
				//TODO throw exception - not indexed
			}
			
			String filterValue;
			
			//endNode in lout of startNode?
			filterValue = startNode.getProperty("Lout").toString();
			if(!filter.check(Long.toString(endNode.getId()), filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}
			
			//startNode in lin of endNode?
			filterValue = endNode.getProperty("Lin").toString();
			if(!filter.check(Long.toString(startNode.getId()), filterValue)){
				Stream<Reachability> result = Stream.of(new Reachability(false));
				return result;
			}
		}
			

		// 7) so far true, lets check children (BSF)
		// TODO: use Cycle Infos for faster searc
		ArrayList<Long> visited = new ArrayList<>();
		ArrayList<Node> adjacentsList = new ArrayList<>();
		String filterValue = "";
		String nodeId = "";

		Iterable<Relationship> children = startNode.getRelationships(Direction.OUTGOING);
		for(Relationship r : children){
			Node n = r.getEndNode();
			adjacentsList.add(n);
		}

		//BSF
		while(adjacentsList.size()>0){
			Node curNode = adjacentsList.get(0);
			adjacentsList.remove(0);
			children = curNode.getRelationships(Direction.OUTGOING);

			if(curNode.getId() == endNode.getId()){
				Stream<Reachability> result = Stream.of(new Reachability(true));
				return result;
			}

			for(Relationship r : children){
				Node m = r.getEndNode();
				if(!visited.contains(m.getId())){
					visited.add(m.getId());
					adjacentsList.add(m);

					//check reachability of m
					//checking if m is in Lin of endNode
					if(endNode.hasProperty("Lin")){
						filterValue = endNode.getProperty("Lin").toString();
					} else {
						//TODO throw exception - not indexed
					}
					nodeId = Long.toString(m.getId());
					if(!filter.check(nodeId, filterValue)){
						Stream<Reachability> result = Stream.of(new Reachability(false));
						return result;
					}

					//checking if endNode is in Lout of m
					if(m.hasProperty("Lout")){
						filterValue = m.getProperty("Lout").toString();
					} else {
						//TODO throw exception - not indexed
					}
					nodeId = Long.toString(endNode.getId());
					if(!filter.check(nodeId, filterValue)){
						Stream<Reachability> result = Stream.of(new Reachability(false));
						return result;
					}


				}
			}

		}

		System.out.println("ERROR");
		return null;
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