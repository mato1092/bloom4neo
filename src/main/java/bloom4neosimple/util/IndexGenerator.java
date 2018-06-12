package bloom4neosimple.util;


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
		//indexation for each node
		for (Node n: dbs.getAllNodes()) {

			long nodeID = n.getId();
			//System.out.println("Start indexation of node " + n.toString());
			//node in cycle --> use nodeRepId of filtering
			if(n.hasProperty("cycleRepID")){
				nodeID = Long.valueOf(n.getProperty("cycleRepID").toString());
			}

			Set<Long> visited = new HashSet<>();
			Queue<Node> adjacentsList = new LinkedList<>();

			//mark the node is indexation was successfully
			//n.setProperty("index", true);
			adjacentsList.add(n);

			//todo: discuss - adding node self --> cycle node to cycle node? really node to cycle node?
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
						/*
						* Lin and Lout extra ...
						* Change Lin of endNode (if cylce get cycle superNode ..)
						* Change Lout of startNode (if start node in a cylce get superNode ..)
						*
						* */

						if(endNode.hasProperty("cycleRepID")){
							endNode = dbs.
									getNodeById(Long.valueOf(endNode.getProperty("cycleRepID").toString()));
						}

						//setLin of children
						setLin(endNode, nodeID);

						//setLout of node
						//todo:
						/*
						* dont look in every iteration in the dbs and search the root node ... -.-
						 */
						Node root = dbs.getNodeById(nodeID);
						setLout(root, endNode.getId());

					}
				}
			}
		}

	}
	
	
	
	/**
	 * Adding nodeID to Lin of n
	 * @param n --> node with property Lin
	 *@param nodeId --> will be added to filter
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
	 * @param n --> node with Property Lout
	 * @param nodeId --> will be added to filter
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