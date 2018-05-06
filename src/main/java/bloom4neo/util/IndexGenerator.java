package bloom4neo.util;


import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public abstract class IndexGenerator {
	
	private static BloomFilter filter = new BloomFilter();

	public static void generateIndex(GraphDatabaseService dbs) {
		
		//todo just get all nodes without lin or lout property -> maybe it will make the indexation a little bit faster
		for (Node n: dbs.getAllNodes()) {
			
			Set<Long> visited = new HashSet<>();
			Queue<Node> adjacentsList = new LinkedList<>();
			long nodeID;

			// Wenn der gerade Indexierte Node in einem Zyklus ist, wird die Zyklus-Node-Id statt seiner NodeId verwendet
			if (n.hasProperty("cycleRepID")) {
				nodeID = (long) n.getProperty("cycleRepID");
				System.out.println("Start indexation of node " + n.toString() + " with cycleRepID " + nodeID);
			} else {
				nodeID = n.getId();
				System.out.println("Start indexation of node " + n.toString());
			}
			
			//mark the node is indexation was successfully
			n.setProperty("index", true);
			adjacentsList.add(n);
			
			//adding filter node self Lin
			setLin(n, nodeID);
			//adding filter node self Lout
			setLout(n, nodeID);

			// BFS
			while(adjacentsList.size() > 0){
				Node curNode = adjacentsList.poll();

				Iterable<Relationship> children = curNode.getRelationships(Direction.OUTGOING);
				for(Relationship r : children){
					if(!visited.contains(r.getEndNodeId())){
						adjacentsList.add(r.getEndNode());
						visited.add(r.getEndNodeId());
						
						Node endNode= r.getEndNode();

						//hint: curNode == parent node && r.getEndNode() == current children

						if(endNode.hasProperty("cycleRepID")){ 
							setLin(endNode, nodeID);
							setLout(n, (long) endNode.getProperty("cycleRepID"));
						} else {
							setLin(endNode, nodeID);
							setLout(n, endNode.getId());
						}
					}
				}
			}
		}
		
		// For Debugging
		for (Node n: dbs.getAllNodes()) {
			if (n.hasProperty("Lin")) {
				System.out.println(n.getProperty("Lin"));
			}
		}

	}
	
	
	
	/**
	 * Adding nodeID to Lin of n
	 * @param n
	 * @param nodeId
	 */
	private static void setLin(Node n, long nodeId) {
		String nodeIDString = Long.toString(nodeId);
		String filterValue = "";
		if(n.hasProperty("Lin")){
			filterValue = n.getProperty("Lin").toString();
		}
		n.setProperty("Lin", filter.add(nodeIDString, filterValue));
	}
	
	
	/**
	 * Adding nodeID to Lout of n
	 * @param n
	 * @param nodeId
	 */
	private static void setLout(Node n, long nodeId) {
		String nodeIDString = Long.toString(nodeId);
		String filterValue = "";
		if(n.hasProperty("Lout")){
			filterValue = n.getProperty("Lout").toString();
		}
		n.setProperty("Lout", filter.add(nodeIDString, filterValue));
	}
	




}