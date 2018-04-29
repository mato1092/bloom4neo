package util;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public abstract class IndexGenerator {
	
	/**
	 * Written by Zoltan
	 * Indexing algorithm based on "Reachability Querying: Can It Be Even Faster?" paper
	 * 
	 * Step 1: DFS through DB to compute Ldis and Lfin of vertices and create post-order
	 * Step 2: Vertices merging based on post-order, MergeIDs of vertices calculated
	 * Step 3: Use hash function to compute Bloom filter index for each MergeID
	 * Step 4: Computation of Bloom filters Lin and Lout for vertices
	 * 
	 */
	public static void generateIndex(GraphDatabaseService dbs) {
		
		// Step 1: DFS through DB to compute Ldis and Lfin of vertices and create post-order
		DepthFirstSearch dfs = new DepthFirstSearch(dbs);
		List<Long> postOrder = dfs.executeDFS();
		
	}
	
	

}
