package bloom4neo.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class BloomFilter {
	
	
	/**
	 * Adds two Bloom filters by bitwise OR and returns the result as String 
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
	
	public static boolean checkBit(int index, byte[] bf) {
		BigInteger bInt = new BigInteger(bf);
		return bInt.testBit(index);
	}
	
	/**
	 * Bloom filter-based reachability query: does a path 1~>2 exist?
	 * return true: path possibly exists
	 * return false: path does not exist
	 * 
	 * @param lin1	Lin of 1st vertex
	 * @param lout1	Lout of 1st vertex
	 * @param lin2	Lin of 2nd vertex
	 * @param lout2	Lout of 2nd vertex
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
		
		return true;
	}
	
	/**
	 * Computes BF indices for all nodes in graph based on mergeMap from VertexMerger.
	 * Returns list of lists of nodes; each index corresponds to a BF index:
	 * bfLists.get(i) will give the list of nodeIDs hashed to BF index i
	 * @param mergeMap Map<mergeID, List<nodeID>> given by VertexMerger
	 * @return bfLists
	 */
	public static List<List<Long>> doBFHash(Map<Long, List<Long>> mergeMap) {
		long d = mergeMap.size();
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
		List<Long> mergeIDs = new ArrayList<Long>(mergeMap.keySet());
		/* simple BF hashing to ensure fairly equal distribution of BF indices over mergeIDs
		 * another solution may be a more randomised mapping of BFIDs to mergeIDs at the risk of more uneven distribution?
		 */
		for(int i = 0; i < mergeIDs.size(); i++) {
			bfLists.get((int) (i % s)).addAll(mergeMap.get(mergeIDs.get(i)));
		}
		return bfLists;
	}
	
	/**
	 * Computes and stores Lin & Lout for each indexed node in DB
	 * @param dbs GraphDatabaseService to be used
	 * @param s number of Bloom filter indices
	 */
	public static void computeBFs(GraphDatabaseService dbs, int s) {
		// Size of each Lin and Lout in bytes: s/8 rounded up
		int size = (s+7)/8;
		for(Node n : dbs.getAllNodes()) {
			if(!n.hasProperty("Lout")) {
				computeNodeBF(n, size, Direction.OUTGOING);
			}
		}

		for(Node n : dbs.getAllNodes()) {
			if(!n.hasProperty("Lin")) {
				computeNodeBF(n, size, Direction.INCOMING);
			}
		}
		
	}
	
	/**
	 * Computes Lin or Lout for a given node.
	 * d determines the direction of the algorithm: Direction.INCOMING computes Lin, Direction.OUTGOING computes Lout
	 * Bloom filter of SCCs are only stored on cycle representatives
	 * @param n node
	 * @param s Bloom filter size (in bytes)
	 * @param d direction of search
	 * @return Bloom filter Lout or Lin depending on d
	 */
	private static byte[] computeNodeBF(Node n, int size, Direction d) {
		byte[] bf = new byte[size];
		String property = "Lin";
		if(d == Direction.OUTGOING) {
			property = "Lout";
		}
		// if n neither member nor representative of an SCC
		if (!n.hasProperty("cycleRepID") && !n.hasProperty("cycleMembers")) {
			Node v;
			bf = addNodeToBF((int) n.getProperty("BFID"), bf);
			byte[] bfV;
			for(Relationship r : n.getRelationships(d)) {
				v = r.getEndNode();
				if(!v.hasProperty(property)) {
					bfV = computeNodeBF(v, size, d);
				}
				else {
					bfV = (byte[]) v.getProperty(property);
				}
				bf = addBFs(bf, bfV);
			}
			n.setProperty(property, bf);
		}
		// if n representative of an SCC
		else if(n.hasProperty("cycleMembers")){
			bf = addNodeToBF((int) n.getProperty("BFID"), bf);
			// set of outgoing or incoming neighbours of the SCC of n
			Set<Node> nextNodes = new HashSet<Node>();
			GraphDatabaseService dbs = n.getGraphDatabase();
			List<Long> sccMembers = Arrays.asList((Long[]) n.getProperty("cycleMembers"));
			for(Long nodeID : sccMembers) {
				for(Relationship r: dbs.getNodeById(nodeID).getRelationships(d)) {
					if(!sccMembers.contains(r.getEndNodeId())) {
						nextNodes.add(r.getEndNode());						
					}
				}
			}
			byte[] bfV;
			for(Node v : nextNodes) {
				if(!v.hasProperty(property)) {
					bfV = computeNodeBF(v, size, d);
				}
				else {
					bfV = (byte[]) v.getProperty(property);
				}
				bf = addBFs(bf, bfV);
			}	
			n.setProperty(property, bf);		
		}
		// if n member of an SCC
		else {
			Node cRep = n.getGraphDatabase().getNodeById((long) n.getProperty("cycleRepID"));
			if(!cRep.hasProperty(property)) {
				bf = computeNodeBF(cRep, size, d);
			}
			else bf = (byte[]) cRep.getProperty(property);
		}
		return bf;
	}
}