package util;

import java.math.BigInteger;

public class BloomFilter {
	
	
	/**
	 * Written by Zoltan
	 * Adds two Bloom filters by bitwise OR and returns the result as String 
	 * @param bf1 1st Bloom filter in byte array form
	 * @param bf2 2nd Bloom filter in byte array form
	 * @return result of bitwise OR of the 2 Bloom filters
	 */
	public static byte[] add(byte[] bf1, byte[] bf2) {
		BigInteger bInt1 = new BigInteger(bf1);
		BigInteger bInt2 = new BigInteger(bf2);
		return bInt1.or(bInt2).toByteArray();
	}
	
	/**
	 * Written by Zoltan
	 * Adds a node's Bloom filter ID to an existing Bloom filter
	 * @param bfID Bloom filter ID to be added
	 * @param bf Bloom filter in byte array form
	 * @return result new Bloom filter as String
	 */
	public static byte[] addNode(int bfID, byte[] bf) {
		BigInteger filter = new BigInteger(bf);
		return filter.setBit(bfID).toByteArray();
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

}
