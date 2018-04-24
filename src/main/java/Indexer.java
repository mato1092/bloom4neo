import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class Indexer {
	
	@Context 
	public GraphDatabaseService dbs;
	
	
	@Procedure(name = "createIndex", mode = Mode.WRITE)
	public void procedure_createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);
		
		// 2. TODO: create Index
		
		
	}
	
	@Procedure(name = "checkReachability", mode = Mode.READ)
	public void procedure_checkReachability(Node startNode, Node endNode) {
		// TODO:
	}
	

}
