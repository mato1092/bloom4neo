package util;

import java.math.BigInteger;

public class BloomFilter {
	
	
	/**
	 * Written by Zoltan
	 * Adds two Bloom filters by bitwise OR and returns the result as String 
	 * @param s1 Bloom filter in string form
	 * @param s2 Another Bloom filter in string form
	 * @return result of bitwise OR of s1 and s2
	 */
	public static String add(String s1, String s2) {
		BigInteger bf1 = new BigInteger(s1.getBytes());
		BigInteger bf2 = new BigInteger(s2.getBytes());
		return new String(bf1.or(bf2).toByteArray());
	}
	
	/**
	 * Written by Zoltan
	 * Adds a node's Bloom filter ID to an existing Bloom filter
	 * @param bfID Bloom filter ID to be added
	 * @param s Bloom filter in string form
	 * @return result new Bloom filter as String
	 */
	public static String addNode(int bfID, String s) {
		BigInteger filter = new BigInteger(s.getBytes());
		return new String(filter.setBit(bfID).toByteArray());
	}
	
	/**
	 * Written by Zoltan
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
	
	public static boolean checkBFReachability(String lin1, String lout1, String lin2, String lout2) {
		
		BigInteger in1 = new BigInteger(lin1.getBytes());
		BigInteger out1 = new BigInteger(lout1.getBytes());
		BigInteger in2 = new BigInteger(lin2.getBytes());
		BigInteger out2 = new BigInteger(lout2.getBytes());
		
		/* if A.andNot(B) is not empty, there is a bit set in A which is not set in B, meaning A is not a subset of B
		 * in our case, this check is done for the Bloom filters out2, out1 and in1, in2
		 * if either check returns true, the reachability query returns false
		 */
		if( out2.andNot(out1).compareTo(BigInteger.ZERO) != 0 || in1.andNot(in2).compareTo(BigInteger.ZERO) != 0 ) {
			return false;
		}
		
		return true;
	}

}
