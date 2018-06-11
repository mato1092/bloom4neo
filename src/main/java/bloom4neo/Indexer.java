package bloom4neo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.IndexGeneratorV2;
import bloom4neo.util.NodePairResult;
import bloom4neo.util.Reachability;

public class Indexer {
	
	@Context 
	public GraphDatabaseService dbs;
	
	/**
	 * Performs necessary actions to create the reachability index: <br>
	 * 1. Detect Cycles and Create Cycle-Nodes. <br>
	 * 2. create Index
	 */
	@Procedure(name = "bloom4neo.createIndex", mode = Mode.WRITE)
	public void procedure_createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);
		
		// 2. create Index
		IndexGeneratorV2.generateIndex(dbs);
		
	}
	
	/**
	 * Deletes index and generated SCC representatives 
	 */
	@Procedure(name = "bloom4neo.deleteIndex", mode = Mode.WRITE)
	public void procedure_deleteIndex() {

		for(Node n : dbs.getAllNodes()) {
			
			if(n.hasProperty("cycleMembers")) {
				n.delete();
			}
			else if(n.hasProperty("cycleRepID")) {
				n.removeProperty("cycleRepID");
			}
			else {
				n.removeProperty("Ldis");
				n.removeProperty("Lfin");
				n.removeProperty("Lin");
				n.removeProperty("Lout");
				n.removeProperty("BFID");
			}
		}
		
	}
	
	/**
	 * Checks whether a path between startNode and endNode exists
	 * @param startNode
	 * @param endNode
	 * @return the result as boolean
	 */
	@UserFunction(value = "bloom4neo.checkReachability")
	public boolean procedure_checkReachability(@Name("startNode") Object startNode, @Name("endNode") Object endNode) {
		Reachability reach = new Reachability(dbs);
		boolean res = false;
		// if arguments are node IDs
		if(startNode instanceof Long && endNode instanceof Long) {
			res = reach.query(dbs.getNodeById((long) startNode), dbs.getNodeById((long) endNode));
		}
		// if arguments are nodes
		else if(startNode instanceof Node && endNode instanceof Node) {
			res = reach.query((Node) startNode, (Node) endNode);
		}
//		// if arguments not suitable
//		else {
//			res = null;
//		}
		return res;
	}
	
	/**
	 * Checks reachability between two Node lists - simplified version
	 * @param startNodes
	 * @param endNodes
	 * @return a Stream<NodePairResult> with the results
	 */
	@Procedure(name = "bloom4neo.massReachSimple", mode = Mode.READ)
	public Stream<NodePairResult> procedure_massReachSimple(@Name("startNode") List<Node> startNodes, @Name("endNode") List<Node> endNodes) {
		Reachability reach = new Reachability(dbs);
		List<NodePairResult> res = new ArrayList<NodePairResult>();
		Set<Node> start = new HashSet<Node>(startNodes);
		Set<Node> end = new HashSet<Node>(endNodes);
		for(Node a : start) {
			for(Node b: end) {
				if(reach.query(a, b)) {
					NodePairResult tba = new NodePairResult(a, b);
					res.add(tba);
				}
			}
		}
		
		return res.stream();
	}
	/**
	 * Checks reachability between two Node lists - terminates when first path is found
	 * @param startNodes
	 * @param endNodes
	 * @return the result as boolean
	 */
	@UserFunction(value = "bloom4neo.massReachBoolean")
	public boolean procedure_massReachFunction(@Name("startNode") List<Node> startNodes, @Name("endNode") List<Node> endNodes) {
		Reachability reach = new Reachability(dbs);
		boolean res = false;
		Set<Node> start = new HashSet<Node>(startNodes);
		Set<Node> end = new HashSet<Node>(endNodes);
		for(Node a : start) {
			for(Node b: end) {
				if(reach.query(a, b)) {
					res = true;
					break;
				}
			}
			if(res) {
				break;
			}
		}
		return res;
	}
	/**
	 * Checks reachability between two Node lists
	 * @param startNodes
	 * @param endNodes
	 * @return a Stream<NodePairResult> with the results
	 */
	@Procedure(name = "bloom4neo.massReachability", mode = Mode.READ)
	public Stream<NodePairResult> procedure_massReachability(@Name("startNode") List<Node> startNodes, @Name("endNode") List<Node> endNodes) {
		Reachability reach = new Reachability(dbs);
		List<NodePairResult> res = new ArrayList<NodePairResult>();
		/*
		 * start of cycle handling part
		 * TODO: this seems overly complex; see whether this can be simplified and whether it's worth the trouble
		 */
		// startNodes with cycle representatives instead of cycle members
		Set<Node> start = new HashSet<Node>();
		// endNodes with cycle representatives instead of cycle members
		Set<Node> end = new HashSet<Node>();
		// maps cycle members from startNodes to their representatives
		Map<Long, Set<Node>> startCycleMap = new HashMap<Long, Set<Node>>();
		// maps cycle members from startNodes to their representatives
		Map<Long, Set<Node>> endCycleMap = new HashMap<Long, Set<Node>>();
		long cycleRepID;
		for(Node n : startNodes) {
			if(n.hasProperty("cycleRepID")) {
				cycleRepID = (long) n.getProperty("cycleRepID");
				if(startCycleMap.containsKey(cycleRepID)) {
					startCycleMap.get(cycleRepID).add(n);
				}
				else {
					start.add(dbs.getNodeById(cycleRepID));
					Set<Node> newCycle = new HashSet<Node>();
					newCycle.add(n);
					startCycleMap.put(cycleRepID, newCycle);
				}
			}
			else {
				start.add(n);
			}
		}
		for(Node n : endNodes) {
			if(n.hasProperty("cycleRepID")) {
				cycleRepID = (long) n.getProperty("cycleRepID");
				if(endCycleMap.containsKey(cycleRepID)) {
					endCycleMap.get(cycleRepID).add(n);
				}
				else {
					end.add(dbs.getNodeById(cycleRepID));
					Set<Node> newCycle = new HashSet<Node>();
					newCycle.add(n);
					endCycleMap.put(cycleRepID, newCycle);
				}
			}
			else {
				end.add(n);
			}
		}
		// if nodes from a cycle come up in both startNodes and endNodes, add these pairs to the result
		for(Long id : endCycleMap.keySet()) {
			if(startCycleMap.keySet().contains(id)) {
				for(Node n : startCycleMap.get(id)) {
					for(Node m : endCycleMap.get(id)) {
						NodePairResult tba = new NodePairResult(n, m);
						res.add(tba);
					}
				}
			}
		}
		/*
		 * end of cycle handling part
		 */
		for(Node a : start) {
			for(Node b: end) {
				// if not added yet
				if(!(startCycleMap.keySet().contains(a.getId()) && a.getId() == b.getId())) {
					if(reach.query(a, b)) {
						if(startCycleMap.keySet().contains(a.getId())) {
							if(endCycleMap.keySet().contains(b.getId())) {
								for(Node n : startCycleMap.get(a.getId())) {
									for(Node m : startCycleMap.get(b.getId())) {
										NodePairResult tba = new NodePairResult(n, m);
										res.add(tba);
									}
								}
							}
							else {
								for(Node n : startCycleMap.get(a.getId())) {
									NodePairResult tba = new NodePairResult(n, b);
									res.add(tba);
								}
							}
						}
						else {
							if(endCycleMap.keySet().contains(b.getId())) {
								for(Node n : startCycleMap.get(a.getId())) {
									NodePairResult tba = new NodePairResult(a, n);
									res.add(tba);
								}
							}
							else{
								NodePairResult tba = new NodePairResult(a, b);
								res.add(tba);
							}
						}
					
					}
				}
			}
		}
		
		return res.stream();
	}
}
