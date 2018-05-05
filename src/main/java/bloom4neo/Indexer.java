package bloom4neo;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.IndexGenerator;
import bloom4neo.util.IndexGeneratorV2;
import bloom4neo.util.ReachResult;

public class Indexer {
	
	@Context 
	public GraphDatabaseService dbs;
	
	/**
	 * Performs necessary actions to create the reachability index: <br>
	 * 1. Detect Cycles and Create Cycle-Nodes. <br>
	 * TODO ...
	 */
	@Procedure(name = "createIndex", mode = Mode.WRITE)
	public void procedure_createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);
		
		// 2. create Index
		IndexGeneratorV2.generateIndex(dbs);
		
	}
	
	/**
	 * Returns true if there is an Path between startNode and endNode
	 * @param startNode
	 * @param endNode
	 */
	@Procedure(name = "checkReachability", mode = Mode.READ)
	public Stream<ReachResult> procedure_checkReachability(@Name("startNode") Node startNode, @Name("endNode") Node endNode) {
		// TODO:
		ReachResult result = new ReachResult(false);
		return Stream.of(result);
	}
	

}
