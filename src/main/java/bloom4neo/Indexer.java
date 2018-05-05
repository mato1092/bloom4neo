package bloom4neo;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.Dummy;
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
	 * Checks whether a path between startNode and endNode exists <br>
	 * TODO: proper implementation instead of this ugly workaround
	 * @param startNode
	 * @param endNode
	 * @return a Stream<Dummy> with 2 elements if path exists, 1 if not 
	 */
	@Procedure(name = "checkReachability", mode = Mode.READ)
	public Stream<Dummy> procedure_checkReachability(@Name("startNode") long startNodeID, @Name("endNode") long endNodeID) {
		Reachability reach = new Reachability();
		Dummy dummy = new Dummy();
		if(reach.query(dbs.getNodeById(startNodeID), dbs.getNodeById(endNodeID))) {
			return Stream.of(dummy, dummy);
		}
		else {
			return Stream.of(dummy);
		}
	}
	

}
