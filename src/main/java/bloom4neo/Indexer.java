package bloom4neo;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.ReachQueryResult;
import bloom4neo.util.IndexGeneratorV2;
import bloom4neo.util.MassReachResult;
import bloom4neo.util.Reachability;

public class Indexer {
	
	@Context 
	public GraphDatabaseService dbs;
	
	/**
	 * Performs necessary actions to create the reachability index: <br>
	 * 1. Detect Cycles and Create Cycle-Nodes. <br>
	 * 2. create Index
	 */
	@Procedure(name = "createIndex", mode = Mode.WRITE)
	public void procedure_createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);
		
		// 2. create Index
		IndexGeneratorV2.generateIndex(dbs);
		
	}
	
	/**
	 * Deletes index and generated SCC representatives 
	 */
	@Procedure(name = "deleteIndex", mode = Mode.WRITE)
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
	 * @return a Stream<ReachQueryResult> with the result as boolean
	 */
	@UserFunction(value = "bloom4neo.checkReachability")
	public boolean procedure_checkReachability(@Name("startNode") Object startNode, @Name("endNode") Object endNode) {
		Reachability reach = new Reachability();
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
	
//	/**
//	 * Checks which pairs of Nodes have paths between two Node lists 
//	 * @param startNodes
//	 * @param endNodes
//	 * @return a Stream<MassReachResult> with the results as pairs of nodes
//	 */
//	@Procedure(name = "massReachability", mode = Mode.READ)
//	public Stream<MassReachResult> procedure_massReachability(@Name("startNode") List<Node> startNodes, @Name("endNode") List<Node> endNodes) {
//		Reachability reach = new Reachability();
//		MassReachResult res = new MassReachResult();
//
//		return Stream.of(res);
//	}

}
