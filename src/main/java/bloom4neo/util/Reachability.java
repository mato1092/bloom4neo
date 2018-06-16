package bloom4neo.util;

import java.util.LinkedList;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Reachability {
	private Set<Long> visitedNodes;
	private GraphDatabaseService dbs;
	LinkedList<Node> queue = new LinkedList<Node>();
	
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
		Node u = startNode;
		Node v = endNode;
		// TODO: check SCC members as well!!!
		// TODO: otherwise not all reachable pairs are found!!!
		// if startNode SCC member, work with representative instead
		if(startNode.hasProperty("cycleRepID")) {
			u = dbs.getNodeById((long) startNode.getProperty("cycleRepID"));
		}
		// if endNode SCC member, work with representative instead
		if(endNode.hasProperty("cycleRepID")) {
			v = dbs.getNodeById((long) endNode.getProperty("cycleRepID"));
		}
		if(u.getId() == v.getId()) {
			return true;
		}
		else {
			queue.clear();
			queue.push(u);
			visitedNodes.clear();
			visitedNodes.add(u.getId());
			return doIterativeQuery(queue, v);
		}
	}
	
	/**
	 * Reachability query iterative inner call
	 * @param u
	 * @param v
	 * @return
	 */
	private boolean doIterativeQuery(LinkedList<Node> queue, Node v) {
		Node u;
		while(!queue.isEmpty()) {
			u = queue.pop();
			// Check reachability based on Ldis and Lfin from DFS
			if(u.hasProperty("Ldis") && v.hasProperty("Ldis") && u.hasProperty("Lfin") && v.hasProperty("Lfin")){
				if((long) u.getProperty("Ldis") <= (long) v.getProperty("Ldis") && (long) u.getProperty("Lfin") >= (long) v.getProperty("Lfin")) {
					queue.clear();
					return true;
				}
			}

			// Check reachability based on Bloom filters
			if(BloomFilter.checkBFReachability((byte[]) u.getProperty("Lin"), (byte[]) u.getProperty("Lout"),
					(byte[]) v.getProperty("Lin"), (byte[]) v.getProperty("Lout"))) {
				// if u ~> v exists based on Bloom filter, do DFS through graph
				// 	if u is not part of an SCC, add undiscovered successors to queue
				if(!u.hasProperty("cycleMembers")) {
					for(Relationship r : u.getRelationships(Direction.OUTGOING)) {
						long id = r.getEndNodeId();
						if(r.getEndNode().hasProperty("cycleRepID")) {
							id = (long) r.getEndNode().getProperty("cycleRepID");
						}
						if(!wasVisited(id)) {
							queue.push(dbs.getNodeById(id));
							visitedNodes.add(id);
						}
					}
				}
				// 	if u is SCC representative, DFS through SCCs undiscovered successors
				else {
					for(long s : CycleNodesGenerator.findNeighbours(u, Direction.OUTGOING)) {
						long id = s;
						if(dbs.getNodeById(s).hasProperty("cycleRepID")) {
							id = (long) dbs.getNodeById(s).getProperty("cycleRepID");
						}
						if(v.getId() == s || v.getId() == id) {
							queue.clear();
							return true;
						}
						if(!wasVisited(id)) {
							queue.push(dbs.getNodeById(id));
							visitedNodes.add(id);
						}
					}
				}
			}
		} 
		return false;
	}
	
	/**
	 * Reachability query inner call
	 * @param u
	 * @param v
	 * @return
	 */
	private boolean doQuery(Node u, Node v) {
		addVisited(u);
//		// if u SCC representative, add all SCC members to visitedNodes
//		// currently not needed
//		if(u.hasProperty("cycleMembers")) {
//			for(long id : (long[]) u.getProperty("cycleMembers")) {
//				addVisited(id);
//			}
//		}
		
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
				long id = r.getEndNodeId();
				if(r.getEndNode().hasProperty("cycleRepID")) {
					id = (long) r.getEndNode().getProperty("cycleRepID");
				}
				if(!wasVisited(id)) {
					// if there is a path u -> x ~> v, return true
					if(doQuery(dbs.getNodeById(id), v)) {
						return true;
					}
				}
			}
		}
		// 	if u is SCC representative, DFS through SCCs undiscovered successors
		else {
			for(long s : CycleNodesGenerator.findNeighbours(u, Direction.OUTGOING)) {
				long id = s;
				if(dbs.getNodeById(s).hasProperty("cycleRepID")) {
					id = (long) dbs.getNodeById(s).getProperty("cycleRepID");
				}
				if(!wasVisited(id)) {
					// if there is a path u -> x ~> v, return true
					if(doQuery(dbs.getNodeById(id), v)) {
						return true;
					}
				}
			}
		}
		// if no path found through DFS, return false
		return false;
	}

//	private void addVisited(long nodeID) {
//		this.visitedNodes.add(nodeID);
//	}
	
	private void addVisited(Node n) {
		this.visitedNodes.add(n.getId());
	}

//	private boolean wasVisited(Node n) {
//		return this.visitedNodes.contains(n.getId());
//	}
	
	private boolean wasVisited(long nodeID) {
		return this.visitedNodes.contains(nodeID);
	}
}