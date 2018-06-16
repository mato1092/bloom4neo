package bloom4neo.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class BloomFilter {
	
	
	/**
	 * Adds two Bloom filters by bitwise OR
	 * @param bf1 1st Bloom filter in byte array form
	 * @param bf2 2nd Bloom filter in byte array form
	 * @return result of bitwise OR of the 2 Bloom filters
	 */
	public static byte[] addBFs(byte[] bf1, byte[] bf2) {
		BigInteger bInt1 = new BigInteger(bf1);
		BigInteger bInt2 = new BigInteger(bf2);
		return bInt1.or(bInt2).toByteArray();
	}
	
	/**
	 * Adds a node's Bloom filter ID to an existing Bloom filter
	 * @param bfID Bloom filter ID to be added
	 * @param bf Bloom filter in byte array form
	 * @return result new Bloom filter as byte array
	 */
	public static byte[] addNodeToBF(int bfID, byte[] bf) {
		BigInteger filter = new BigInteger(bf);
		return filter.setBit(bfID).toByteArray();
	}
	
	/**
	 * Checks whether the bit at a specific index is set in a Bloom filter
	 * @param index index to be checked
	 * @param bf Bloom filter
	 * @return true if set, false otherwise
	 */
	public static boolean checkBit(int index, byte[] bf) {
		BigInteger bInt = new BigInteger(bf);
		return bInt.testBit(index);
	}
	
	/**
	 * Bloom filter-based reachability query: does a path 1~>2 exist?
	 * path possibly exists: return true;
	 * path does not exist: return false
	 * 
	 * @param lin1	Lin of 1st node
	 * @param lout1	Lout of 1st node
	 * @param lin2	Lin of 2nd node
	 * @param lout2	Lout of 2nd node
	 * @return result of reachability query
	 */
	
	public static boolean checkBFReachability(byte[] lin1, byte[] lout1, byte[] lin2, byte[] lout2) {
		
		BigInteger in1 = new BigInteger(lin1);
		BigInteger out1 = new BigInteger(lout1);
		BigInteger in2 = new BigInteger(lin2);
		BigInteger out2 = new BigInteger(lout2);
		
		/* if A.andNot(B) is not empty, there is a bit set in A which is not set in B, meaning A is not a subset of B
		 * in our case, this check is done for the Bloom filters out2, out1 and in1, in2
		 * if either check returns true, the reachability query returns false
		 */
		if( out2.andNot(out1).compareTo(BigInteger.ZERO) != 0 || in1.andNot(in2).compareTo(BigInteger.ZERO) != 0 ) {
			return false;
		}
		else return true;
	}
	
	/**
	 * Computes BF indices for all nodes in graph based on mergeMap from VertexMerger.
	 * Returns list of lists of nodes; each index corresponds to a BF index:
	 * bfLists.get(i) will give the list of nodeIDs hashed to BF index i
	 * @param mergeMap Map(mergeID, List(nodeID)) given by VertexMerger
	 * @return bfLists
	 */
	public static List<List<Long>> doBFHash(List<List<Long>> mergeList) {
		long d = mergeList.size();
		long s = 160;
		if(d <= 10) {
			s = d;
		}
		else if(d <= 100) {
			s = d/2;
		}
		else if(d < 1600) {
			s = d/5;
		}
		List<List<Long>> bfLists = new ArrayList<List<Long>>();
		for(int i = 0; i < s; i++) {
			bfLists.add(new ArrayList<Long>());			
		}
		/* simple BF hashing to ensure fairly equal distribution of BF indices over mergeIDs
		 * another solution may be a more randomised mapping of BFIDs to mergeIDs at the risk of more uneven distribution?
		 */
		for(int i = 0; i < mergeList.size(); i++) {
			bfLists.get((int) (i % s)).addAll(mergeList.get(i));
		}
		return bfLists;
	}
	/**
	 * Computes BF indices for all nodes in graph based on mergeMap from VertexMerger.
	 * Writes BFIDs straight to graph
	 * @param mergeMap Map(mergeID, List(nodeID)) given by VertexMerger
	 * @return bfLists
	 */
	public static void doBFHashToGraph(List<List<Long>> mergeList, GraphDatabaseService dbs) {
		int d = mergeList.size();
		int s = 160;
		if(d <= 10) {
			s = d;
		}
		else if(d <= 100) {
			s = d/2;
		}
		else if(d < 1600) {
			s = d/5;
		}
		/* simple BF hashing to ensure fairly equal distribution of BF indices over mergeIDs
		 * another solution may be a more randomised mapping of BFIDs to mergeIDs at the risk of more uneven distribution?
		 */
		for(int i = 0; i < mergeList.size(); i++) {
			for(long id : mergeList.get(i)) {
				dbs.getNodeById(id).setProperty("BFID", i % s);
			}
//			bfLists.get((int) (i % s)).addAll(mergeList.get(i));
		}
	}
	/**
	 * Variant of doBFHash using 2D arrays
	 * @param mergeArray 2D array of VertexMerger results
	 * @return bfArray
	 */
	public static long[][] doArrayBFHash(long[][] mergeArray) {
		int d = mergeArray.length;
		int s = 160;
		int mergeSize = mergeArray[0].length;
		if(d < 6) {
			s = d;
		}
		else if(d <= 100) {
			s = (d + 1) / 2;
		}
		else if(d < 1600) {
			s = (d + 4) / 5;
		}
		long[][] bfArray = new long[s][(mergeSize * d + s - 1) / s];
		for(long[] row : bfArray) {
			Arrays.fill(row, -1);
		}
		/* simple BF hashing to ensure fairly equal distribution of BF indices over mergeIDs
		 * another solution may be a more randomised mapping of BFIDs to mergeIDs at the risk of more uneven distribution?
		 */
		for(int i = 0; i < mergeArray.length; i++) {
			for(int j = 0; j < mergeArray[i].length; j++) {
				bfArray[i % s][mergeSize * (i / s) + j] = mergeArray[i][j];
			}
		}
//		// resulting bfArray can have "empty spots" that contain -1 where no value was copied in
//		// if the next step (storing BFIDs to nodes in IndexGeneratorV2) would not consider this, they would have to be removed:
//		long[] shortened;
//		for(int i = 0; i < s; i++) {
//			for(int j = 0; j < bfArray[0].length; j++) {
//				if(bfArray[i][j] == -1) {
//					shortened = new long[j];
//					System.arraycopy(bfArray[i], 0, shortened, 0, j);
//					bfArray[i] = shortened;
//					break;
//				}
//			}
//		}
		return bfArray;
	}
	
	/**
	 * Computes and stores Lin & Lout for each indexed node in DB
	 * @param dbs GraphDatabaseService to be used
	 */
	public static void computeBFs(GraphDatabaseService dbs) {
		for(Node n : dbs.getAllNodes()) {
			if(!n.hasProperty("Lout")) {
				computeNodeBF(n, Direction.OUTGOING);
			}
		}

		for(Node n : dbs.getAllNodes()) {
			if(!n.hasProperty("Lin")) {
				computeNodeBF(n, Direction.INCOMING);
			}
		}
		
	}
	
	/**
	 * Computes Lin or Lout for a given node.
	 * d determines the direction of the algorithm: Direction.INCOMING computes Lin, Direction.OUTGOING computes Lout
	 * Bloom filters of SCCs are only stored on cycle representatives
	 * @param n node
	 * @param d direction of search
	 * @return Bloom filter Lout or Lin depending on d
	 */
	private static byte[] computeNodeBF(Node n, Direction d) {
		byte[] bf = new byte[]{0};
		String property;
		String cycleDegree;
		if(d == Direction.OUTGOING) {
			property = "Lout";
			cycleDegree = "outDegree";
		}
		else {
			property = "Lin";
			cycleDegree = "inDegree";
		}
		
		// if n representative of an SCC
		if(n.hasProperty("cycleMembers")){
			if(n.hasProperty("BFID")){
				bf = addNodeToBF((int) n.getProperty("BFID"), bf);
			}

			n.setProperty(property, bf);
			if((long) n.getProperty(cycleDegree) != 0) {
				// set of outgoing or incoming neighbours of the SCC of n
				Set<Node> nextNodes = CycleNodesGenerator.findNeighbourNodes(n, d);
				byte[] bfV;
				for(Node v : nextNodes) {
					if(!v.hasProperty(property)) {
						bfV = computeNodeBF(v, d);
					}
					else {
						bfV = (byte[]) v.getProperty(property);
					}
					bf = addBFs(bf, bfV);
					n.setProperty(property, bf);
				}	
			}		
		}
		// if n member of an SCC
		else if(n.hasProperty("cycleRepID")){
			Node cRep = n.getGraphDatabase().getNodeById((long) n.getProperty("cycleRepID"));
			if(!cRep.hasProperty(property)) {
				bf = computeNodeBF(cRep, d);
			}
			else {
				bf = (byte[]) cRep.getProperty(property);
			}
		}
		// if n not part of an SCC
		else {
			if(n.hasProperty("BFID")){
				bf = addNodeToBF((int) n.getProperty("BFID"), bf);
			}

			n.setProperty(property, bf);
			if(n.getDegree(d) != 0) {
				Node v;
				byte[] bfV;
				for(Relationship r : n.getRelationships(d)) {
					if(d == Direction.OUTGOING) {
						v = r.getEndNode();
					}
					else {
						v = r.getStartNode();
					}
					if(n.getId() != v.getId()) {
						if(!v.hasProperty(property)) {
							bfV = computeNodeBF(v, d);
						}
						else {
							bfV = (byte[]) v.getProperty(property);
						}
						bf = addBFs(bf, bfV);
						n.setProperty(property, bf);
					}
				}
			}
		}
		return bf;
	}
}