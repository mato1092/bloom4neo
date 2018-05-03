package indexation.util;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;

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
		// Step 2: Vertex merging
		VertexMerger vm = new VertexMerger(postOrder);
		Map<Long, List<Long>> mergeMap = vm.merge();
		// Step 3: Bloom filter hashing
		List<List<Long>> bfLists;
		bfLists = BloomFilter.doBFHash(mergeMap);
		// Storing Bloom filter index to each node in DB
		int s = bfLists.size();
		for(int i = 0; i < s; i++) {
			List<Long> nodeList = bfLists.get(i);
			for(int j = 0; j < nodeList.size(); j++) {
				dbs.getNodeById(nodeList.get(j)).setProperty("BFID", i);
			}
		}
		// Step 4: Computing Lin and Lout for each node
		BloomFilter.computeBFs(dbs, s);
		
	}
	
	

}
