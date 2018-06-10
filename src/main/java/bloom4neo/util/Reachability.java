package bloom4neo.util;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Reachability {
	private Set<Long> visitedNodes;
	private GraphDatabaseService dbs;
	
	public Reachability(GraphDatabaseService gdbs) {
		this.visitedNodes = new HashSet<Long>();
		this.dbs = gdbs;
	}

	/**
	 * Reachability query: is there a path startNode ~> endNode?
	 * @param startNode
	 * @param endNode
	 * @return
	 */
	public boolean query(Node startNode, Node endNode) {
		clearVisited();
		Node v = endNode;
		// if endNode SCC member, work with representative instead
		if(endNode.hasProperty("cycleRepID")) {
			v = dbs.getNodeById((long) endNode.getProperty("cycleRepID"));
		}
		return doQuery(startNode, v);
	}
	
	/**
	 * Reachability query inner call
	 * @param startNode
	 * @param endNode
	 * @return
	 */
	private boolean doQuery(Node startNode, Node endNode) {
		Node u;
		Node v = endNode;
		// if startNode SCC member, work with representative instead
		if(startNode.hasProperty("cycleRepID")) {
			u = dbs.getNodeById((long) startNode.getProperty("cycleRepID"));
		}
		else {
			u = startNode;
		}
		addVisited(u);
		// if u SCC representative, add all SCC members to visitedNodes
		if(u.hasProperty("cycleMembers")) {
			for(long id : (long[]) u.getProperty("cycleMembers")) {
				addVisited(id);
			}
		}
		
		// Check reachability based on Ldis and Lfin from DFS
		if((long) u.getProperty("Ldis") <= (long) v.getProperty("Ldis") && (long) u.getProperty("Lfin") >= (long) v.getProperty("Lfin")) {
			return true;
		}
		// Check reachability based on Bloom filters
		if(!BloomFilter.checkBFReachability((byte[]) u.getProperty("Lin"), (byte[]) u.getProperty("Lout"),
				(byte[]) v.getProperty("Lin"), (byte[]) v.getProperty("Lout"))) {
			return false;
		}
		// if u ~> v exists based on Bloom filter, do DFS through graph
		// 	if u is not part of an SCC, DFS through undiscovered successors
		if(!u.hasProperty("cycleMembers")) {
			for(Relationship r : u.getRelationships(Direction.OUTGOING)) {
				if(!wasVisited(r.getEndNodeId())) {
					// if there is a path u -> x ~> v, return true
					if(doQuery(r.getEndNode(), v)) {
						return true;
					}
				}
			}
		}
		// 	if u is SCC representative, DFS through SCCs undiscovered successors
		else {
			for(long s : CycleNodesGenerator.findNeighbours(u, Direction.OUTGOING)) {
				if(!wasVisited(s)) {
					// if there is a path u -> s ~> v, return true
					if(doQuery(dbs.getNodeById(s), v)) {
						return true;
					}
				}
			}
		}
		// if no path found through DFS, return false
		return false;
	}

	private void addVisited(long nodeID) {
		this.visitedNodes.add(nodeID);
	}
	
	private void addVisited(Node n) {
		this.visitedNodes.add(n.getId());
	}
	
	private void clearVisited() {
		this.visitedNodes = new HashSet<Long>();
	}

	private boolean wasVisited(Node n) {
		return this.visitedNodes.contains(n.getId());
	}
	
	private boolean wasVisited(long nodeID) {
		return this.visitedNodes.contains(nodeID);
	}
}
