
package bloom4neo.util;

import java.util.*;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.*;

public abstract class CycleNodesGenerator {
	
	// for All CycleRepNodes:
	// label name for Cycle representatives
	public static enum cycleRepNodeLabel implements Label 
	{
		CYCLE_REP 
	}
	// list of NodeIDs of the members in a Cycle
	public static final String PROPERTY_CYCLE_REP_NODE_MEMBERS = "cycleMembers";

	
	// for all non-CycleRepNodes: 
	// NodeID of the CycleRepNode
	public static final String PROPERTY_CYClE_REP_ID = "cycleRepID";
	
	
	

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
					long[] membersArray = new long[membersList.size()];
					
					// calc Properties of Cycle Rep
					for (int i = 0; i <membersList.size(); i++) {
						Node member = dbs.getNodeById(membersList.get(i));
						membersArray[i] = membersList.get(i);
						// add Property to each member of the cycleRep
						member.setProperty(PROPERTY_CYClE_REP_ID, newCycleRepNode.getId());
					}
					
					// add Properties to CycleRep
					newCycleRepNode.setProperty(PROPERTY_CYCLE_REP_NODE_MEMBERS, membersArray);
				}	
			}
		}
	}

	/**
	 * Finds all neighbours of the cycle represented by n in the direction d
	 * @param n cycle representative
	 * @return set of neighbours
	 */
	public static Set<Node> findNeighbours(Node n){
		Direction d = Direction.OUTGOING;
		Set<Node> neighbours = new HashSet<Node>();
		Node v;
		Set<Long> memberList = new HashSet<>();
		GraphDatabaseService dbs = n.getGraphDatabase();
		if(n.hasProperty("cycleMembers")){
			memberList = new HashSet<Long>(Arrays.asList(ArrayUtils.toObject((long[]) n.getProperty("cycleMembers"))));
		}

		for(Long id : memberList) {
			v = dbs.getNodeById(id);
			for(Relationship r : v.getRelationships(d)) {
				if(d == Direction.OUTGOING) {
					if(!memberList.contains(r.getEndNodeId())) {
						neighbours.add(r.getEndNode());
					}
				}
				else {
					if(!memberList.contains(r.getStartNodeId())) {
						neighbours.add(r.getStartNode());
					}
				}
			}
		}
		return neighbours;
	}

}