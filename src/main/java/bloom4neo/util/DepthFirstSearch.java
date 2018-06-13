package bloom4neo.util;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;

public class DepthFirstSearch {
	
	private long current;
//	private long poCounter;
	private GraphDatabaseService dbs;
	LinkedList<Long> postOrder;
//	private long[] arrayPostOrder;
	private ResourceIterable<Node> allNodes;
	LinkedList<Long> queue = new LinkedList<Long>();
	
	/**
	 * Executes Depth-first search through DB to compute Ldis & Lfin for all nodes and the post order <br>
	 * Ldis and Lfin stored as properties on nodes, for SCC members they are only stored on cycle representatives <br>
	 * @return post order
	 */
	public List<Long> executeIterativeDFS(){
		Node v;
		for(Node n : allNodes) {
			// if n has not been discovered by DFS
			if( !n.hasProperty("Ldis") ) {
				
				/* if (n not SCC representative and has no incoming relationships)
				 * 	or (n is SCC representative and SCC has no incoming relationships)
				 */
				if( ( !n.hasProperty("cycleMembers") && n.getDegree(Direction.INCOMING) == 0 )
						|| ( n.hasProperty("cycleMembers") && (long) n.getProperty("inDegree") == 0) ) {
					queue.push(n.getId());
					while(!queue.isEmpty()) {
						v = dbs.getNodeById(queue.peek());
						if(!v.hasProperty("Lfin")) {
							if(!v.hasProperty("Ldis")) {
								iterativeDFSVisit(v);
							}
							// if n was already found by DFS: set Lfin and remove from queue
							else {
								addToPostOrder(v.getId());
								v.setProperty("Lfin", incrementCurrent());
								queue.pop();
							}
						}
						else {
							queue.pop();
						}
					}			
				}
			}
		}
		return getPostOrder();
		// arrayPostOrder can have empty space at the end which should be deleted
//		long[] shortenedPostOrder = new long[getPoCounter()];
//		System.arraycopy(getArrayPostOrder(), 0, shortenedPostOrder, 0, getPoCounter());
//		return shortenedPostOrder;
	}
	
	private void iterativeDFSVisit(Node n) {
		// if n has not been found by DFS: set Ldis and 
		// if n representative of an SCC
		if(n.hasProperty("cycleMembers")) {
			Set<Node> outList = CycleNodesGenerator.findNeighbourNodes(n, Direction.OUTGOING);
			n.setProperty("Ldis", incrementCurrent());
			for(Node outNode : outList) {
				if(!outNode.hasProperty("Ldis")) {
					queue.push(outNode.getId());
				}
			}
		}
		// if n member of an SCC: replace n in queue with its SCC representative
		else if(n.hasProperty("cycleRepID")) {
			queue.pop();
			Node cRep = dbs.getNodeById((long) n.getProperty("cycleRepID"));
			if(!cRep.hasProperty("Ldis")) {
				queue.push(cRep.getId());
			}
		}
		// if n not part of an SCC
		else {
			n.setProperty("Ldis", incrementCurrent());
			Node v;
			for(Relationship r : n.getRelationships(Direction.OUTGOING)) {
				v = r.getEndNode();
				if(!v.hasProperty("Ldis")) {
					queue.push(v.getId());
				}
			}
		}
	}

//	/**
//	 * Executes Depth-first search through DB to compute Ldis & Lfin for all nodes and the post order <br>
//	 * Ldis and Lfin stored as properties on nodes, for SCC members they are only stored on cycle representatives <br>
//	 * @return post order
//	 */
//	public long[] executeDFS(){
//		
//		for(Node n : allNodes) {
//			
//			// if n has not been discovered by DFS
//			if( !n.hasProperty("Ldis") ) {
//				
//				/* if (n not SCC representative and has no incoming relationships)
//				 * 	or (n is SCC representative and SCC has no incoming relationships)
//				 */
//				if( ( !n.hasProperty("cycleMembers") && n.getDegree(Direction.INCOMING) == 0 )
//						|| ( n.hasProperty("cycleMembers") && (long) n.getProperty("inDegree") == 0) ) {
//					
//					DFSVisit(n);
//					
//				}
//			}
//			
//			
//		}
//		// arrayPostOrder can have empty space at the end which should be deleted
//		long[] shortenedPostOrder = new long[getPoCounter()];
//		System.arraycopy(getArrayPostOrder(), 0, shortenedPostOrder, 0, getPoCounter());
//		return shortenedPostOrder;
//		
//	}  
//	
//	private void DFSVisit(Node n) {
//		// if n representative of an SCC
//		if(n.hasProperty("cycleMembers")) {
//			Set<Node> outList = CycleNodesGenerator.findNeighbourNodes(n, Direction.OUTGOING);
//			n.setProperty("Ldis", incrementCurrent()); 
//			for(Node outNode : outList) {
//				if(!outNode.hasProperty("Ldis")) {
//					DFSVisit(outNode);
//				}
//			}
//			addToPostOrder(n.getId());
//			n.setProperty("Lfin", incrementCurrent());
//		}
//		// if n member of an SCC
//		else if(n.hasProperty("cycleRepID")) {
//			Node cRep = dbs.getNodeById((long) n.getProperty("cycleRepID"));
//			if(!cRep.hasProperty("Ldis")) {
//				DFSVisit(cRep);
//			}
//		}
//		// if n not part of an SCC
//		else {
//			n.setProperty("Ldis", incrementCurrent());
//			Node v;
//			for(Relationship r : n.getRelationships(Direction.OUTGOING)) {
//				v = r.getEndNode();
//				if(!v.hasProperty("Ldis")) {
//					DFSVisit(v);
//				}
//			}
//			addToPostOrder(n.getId());
//			n.setProperty("Lfin", incrementCurrent());
//		}
//	}
	
	public DepthFirstSearch(GraphDatabaseService gdbs) {
		
		this.current = -1;
//		this.poCounter = 0;
		this.dbs = gdbs;
		this.allNodes = gdbs.getAllNodes();
		this.postOrder = new LinkedList<Long>();
		// Size of arrayPostOrder = no. of nodes in DB
		// TODO: use no. of indexable nodes (non-SCC + SCC rep.) instead
//		this.arrayPostOrder = new long[(int) allNodes.stream().count()];
	}
	
	private void addToPostOrder(long nodeID) {
		
		this.postOrder.add(nodeID);
//		this.arrayPostOrder[getAndIncrementPoCounter()] = nodeID;
		
	}
	
	private LinkedList<Long> getPostOrder() {
		
		return this.postOrder;
		
	}
	
//	public long[] getArrayPostOrder() {
//		
//		return this.arrayPostOrder;
//		
//	}
	
//	private int getAndIncrementPoCounter() {
//
//		return (int) this.poCounter++;
//		
//	}
	
//	private int getPoCounter() {
//		
//		return (int) this.poCounter;
//		
//	}
	
	private long incrementCurrent() {
		
		this.current += 1;
		return this.current;
		
	}

}
