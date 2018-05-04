package bloom4neo;

import bloom4neo.util.BloomFilter;
import bloom4neo.util.CycleNodesGenerator;
import bloom4neo.util.IndexGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.procedure.*;

import java.util.stream.Stream;


public class Indexer {
	//TODO config for all node propertys ... example Lout, Lin , etc. ...
	@Context
	public GraphDatabaseService dbs;

	/**
	 * Performs necessary actions to create the reachability index: <br>
	 * 1. Detect Cycles and Create Cycle-Nodes. <br>
	 * TODO ..
	 */

	@Procedure(value = "createIndex", mode=Mode.WRITE)
	@Description("Creating Reachability index for all nodes")
	public void createIndex() {
		// 1. Detect Cycles and Create Cycle-Nodes
		CycleNodesGenerator.generateCycleNodes(dbs);

		// 2. create Index
		IndexGenerator.generateIndex(dbs);

	}

	/**
	 * Returns true if there is an Path between startNode and endNode
	 * @param startNode
	 * @param endNode
	 */
	@Procedure(value = "checkReachability", mode = Mode.READ)
	@Description("Checking Reachability of to nodes")
	public Stream<Reachability> checkReachability(@Name("startNode") Node startNode,
									 @Name("endNode") Node endNode) {
		BloomFilter filter = new BloomFilter();

		//TODO [CYCLE] same cylce? return true

		//check direct connection
		String filterValue = "";
		String nodeId = "";

		//checking lout of startNode
		if(startNode.hasProperty("Lout")){
			filterValue = startNode.getProperty("Lout").toString();
		} else {
			//TODO throw exception
		}
		nodeId = Long.toString(endNode.getId());
		//endNode in lout?
		if(!filter.check(nodeId, filterValue)){
			Stream<Reachability> result = Stream.of(new Reachability(false));
			return result;
		}

		//checking lout of startNode
		if(endNode.hasProperty("Lin")){
			filterValue = endNode.getProperty("Lin").toString();
			} else {
			//TODO throw exception
		}
		nodeId = Long.toString(startNode.getId());
		//startNode in lin?
		if(!filter.check(nodeId, filterValue)){
			Stream<Reachability> result = Stream.of(new Reachability(false));
			return result;
		}




		Stream<Reachability> result = Stream.of(new Reachability(true));
		return result;
	}

	//neo4j need a class, returns of procedures have to be streams
	//can have -> also pass into reachability the path to the node (?)
	public class Reachability {
		public Boolean out;

		public Reachability(Boolean out){
			this.out = out;
		}

		public Boolean getOut(){
			return this.out;
		}

	}


}