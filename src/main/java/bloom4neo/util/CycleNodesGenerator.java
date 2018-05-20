package bloom4neo.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public abstract class CycleNodesGenerator {
	
	// for All CycleRepNodes:
	// label name for Cycle representatives
	public static enum cycleRepNodeLabel implements Label 
	{
		CYCLE_REP 
	}
	// sum # of all incoming transitions of the members of an cycleRep
	public static final String PROPERTY_CYCLE_REP_NODE_INDEGREE = "inDegree"; 
	// sum # of all outgoing transitions of the members of an cycleRep
	public static final String PROPERTY_CYCLE_REP_NODE_OUTEGREE = "outDegree";
	// list of NodeIDs of the members in a Cycle
	public static final String PROPERTY_CYCLE_REP_NODE_MEMBERS = "cycleMembers";

	
	// for all non-CycleRepNodes: 
	// NodeID of the CycleRepNode
	public static final String PROPERTY_NODE_CYLCE_REP_NODE_ID = "cycleRepID";
	
	
	

	/**
	 * Uses algo.scc to detect cycles in the graph. For each cycle
	 * a new Node which represents this cycle is created and added to the graph.
	 * @param dbs
	 */
	public static void generateCycleNodes(GraphDatabaseService dbs) {

		// Call the scc algorithm
		try ( Result result = dbs.execute("CALL algo.scc.stream()")) {
			
			// save all data from scc algorithm in this map
			Map<Long, ArrayList<Long>> cyclesAndMembers = new HashMap<>();
			
			// Identify all Members of Cycles and create CycleRepNodes
			while (result.hasNext()){
				// the result is a pair of long: partitionID and NodeID
				Map<String, Object> row = result.next();
				long partition = (long) row.get("partition");
				long nodeId = (long) row.get("nodeId");
				
				// save this information in the Map
				if (cyclesAndMembers.containsKey(partition)) {
					cyclesAndMembers.get(partition).add(nodeId);
					
				} else {
					ArrayList<Long> newMembersList = new ArrayList<Long>();
					newMembersList.add(nodeId);
					cyclesAndMembers.put(partition, newMembersList);
				}	
			}
			
			// Now we have a Map with partition -> List of Members
			
			// Iterate the Map and create our CycleRepNodes
			for (ArrayList<Long> membersList: cyclesAndMembers.values()) {
				// ignore cycles with just one member
				if (membersList.size() > 1) {
					// create the CycleRep
					Node newCycleRepNode = dbs.createNode(cycleRepNodeLabel.CYCLE_REP);
					long inDegree = 0;
					long outDegree = 0;
					long[] membersArray = new long[membersList.size()];
					
					// calc Properties of Cycle Rep
					for (int i = 0; i <membersList.size(); i++) {
						Node member = dbs.getNodeById(membersList.get(i));
						
						// just count relationships of nodes that are not in the same cycle
						for(Relationship r: member.getRelationships(Direction.INCOMING)) {
							if (!membersList.contains(r.getStartNodeId())){
								inDegree += 1;
							}
						}
						for(Relationship r: member.getRelationships(Direction.OUTGOING)) {
							if (!membersList.contains(r.getEndNodeId())){
								outDegree += 1;
							}
						}
						
						membersArray[i] = membersList.get(i);
						
						// add Property to each member of the cycleRep
						member.setProperty(PROPERTY_NODE_CYLCE_REP_NODE_ID, newCycleRepNode.getId());
					}
					
					// add Properties to CycleRep
					newCycleRepNode.setProperty(PROPERTY_CYCLE_REP_NODE_INDEGREE, inDegree);
					newCycleRepNode.setProperty(PROPERTY_CYCLE_REP_NODE_OUTEGREE, outDegree);
					newCycleRepNode.setProperty(PROPERTY_CYCLE_REP_NODE_MEMBERS, membersArray);
				}	
			}
		}
		
		for (Node n: dbs.getAllNodes()) {
			System.out.println(n.getAllProperties().toString());
		}
	}
	
	/**
	 * Finds all neighbours of the cycle represented by n in the direction d
	 * @param n cycle representative
	 * @param d direction
	 * @return set of neighbours
	 */
	public static Set<Node> findNeighbours(Node n, Direction d){
		Set<Node> neighbours = new HashSet<Node>();
		Node v;
		GraphDatabaseService dbs = n.getGraphDatabase();
		long[] members = (long[]) n.getProperty("cycleMembers");
		Set<Long> memberSet = new HashSet<Long>();
		for (long m : members) {
			memberSet.add(m);
		}
		for(Long id : memberSet) {
			v = dbs.getNodeById(id);
			for(Relationship r : v.getRelationships(d)) {
				if(d == Direction.OUTGOING) {
					if(!memberSet.contains(r.getEndNodeId())) {
						neighbours.add(r.getEndNode());						
					}
				}
				else {
					if(!memberSet.contains(r.getStartNodeId())) {
						neighbours.add(r.getStartNode());						
					}
				}
			}
		}
		return neighbours;
	}

}