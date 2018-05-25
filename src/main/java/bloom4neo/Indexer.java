package bloom4neo;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.ReachQueryResult;
import bloom4neo.util.IndexGeneratorV2;
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
	 * Checks whether a path between startNode and endNode exists
	 * @param startNode
	 * @param endNode
	 * @return a Stream<ReachQueryResult> with the result as boolean
	 */
	@Procedure(name = "checkReachability", mode = Mode.READ)
	public Stream<ReachQueryResult> procedure_checkReachability(@Name("startNode") Object startNode, @Name("endNode") Object endNode) {
		Reachability reach = new Reachability();
		ReachQueryResult res = new ReachQueryResult(false);
		// if arguments are node IDs
		if(startNode instanceof Long && endNode instanceof Long) {
			res = new ReachQueryResult(reach.query(dbs.getNodeById((long) startNode), dbs.getNodeById((long) endNode)));
		}
		// if arguments are nodes
		else if(startNode instanceof Node && endNode instanceof Node) {
			res = new ReachQueryResult(reach.query((Node) startNode, (Node) endNode));
		}
//		// if arguments not suitable
//		else {
//			res = null;
//		}
		return Stream.of(res);
	}
	

}
