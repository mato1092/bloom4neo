package bloom4neo.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang3.StringUtils;

public class BloomFilter {
	
	// length of the Lin and Lout value. Must be a Power of 2!
	private int length = 16;
	
	// for uninitialized Lin or Lout values
	private String init = StringUtils.repeat("0",  length);


	/**
	 * author mri
	 * @param n element which will be hashed
	 * @param value current value of bloomfilter
	 * @return new value of bloomfilter after adding n
	 */
	public String add(String n, String value) {
		int index = this.getIndex(n);
		
		//empty value has to be initialized
		if(value.equals("")){
			value = init;
		}
		
		StringBuilder result = new StringBuilder(value);
		result.setCharAt(index, '1');
		
		return result.toString();
	}

	/**
	 * author mri
	 * check if element matching on the filter
	 * @param n element which will be checked
	 * @param value current value of bloomfilter
	 * @return true | false , true if the element matching , false if not
	 */
	public boolean check(String n, String value) {
		int index = this.getIndex(n);
		
		//empty value has to be initialized
		if(value.equals("")){
			// TODO: should not be empty at this point, throw Exception?
			System.out.println("no value to check, maybe there is an Error in the Indexation");
			value = init;
		}
		
		if (value.charAt(index) == '1') return true;
		return false;
	}

	
	
	/**
	 * author mri
	 * calculate with some fancy hash functions index value
	 * @param n element id
	 * @return index
	 */
	private int getIndex(String n) {
//		return getIndex_SimpleModuloHash(n);
//		return getIndex_JavaHash(n);
//		return getIndex_SHA256Hash(n);
		return getIndex_Murmur3(n);
	}
		
	
	
	
	/////////// Hashing functions ////////////
	
	/**
	 * Simple Modulo Hash-Function
	 * 
	 * @param n String-Value to hash
	 * @return index for Lin/ Lout
	 */
	private int getIndex_SimpleModuloHash(String n) {
		return Integer.parseInt(n) % this.length;
	}
	
	/**
	 * Hashes String with intern Java .hashCode() Function. 
	 * Uses Bit-Wise & to trim the Hash-Value to the length of Lin/Lout.
	 * 
	 * @param n String-Value to hash
	 * @return index for Lin/ Lout
	 */
	private int getIndex_JavaHash(String n) {
		return n.hashCode() & (length -1);
	}
	
	/**
	 * Encrypt n with SHA 256 and hashes the encrypted String with
	 * intern Java .hashCode() Function. 
	 * Uses Bit-Wise & to trim the Hash-Value to the length of Lin/Lout.
	 * 
	 * @param n String-Value to hash
	 * @return index for Lin/ Lout
	 */
	private int getIndex_SHA256Hash(String n) {
		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
			messageDigest.update(n.getBytes());
			String encryptedString = new String(messageDigest.digest());
			return encryptedString.hashCode() & (length -1);
			
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return getIndex_JavaHash(n);
		}
	}
	
	/**
	 * Uses Murmur3 Hash-Function.
	 * Uses Bit-Wise & to trim the Hash-Value to the length of Lin/Lout. <br>
	 * Implementation from: 
	 * https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/Murmur3.java
	 * 
	 * @param n String-Value to hash
	 * @return index for Lin/ Lout
	 */
	private int getIndex_Murmur3(String n) {
		// Constants for 32 bit variant
		final int C1_32 = 0xcc9e2d51;
		final int C2_32 = 0x1b873593;
		final int R1_32 = 15;
		final int R2_32 = 13;
		final int M_32 = 5;
		final int N_32 = 0xe6546b64;
		
		final int seed = 0;
		final int offset = 0;
		final int stringLength = n.length();
		final int nBlocks = stringLength >> 2;
		
		int hash = seed;
		byte[] data = n.getBytes();
		
		// body
		for (int i = 0; i < nBlocks; i++) {
			int i_4 = i << 2;
			int k = (data[offset + i_4] & 0xff)
			    | ((data[offset + i_4 + 1] & 0xff) << 8)
			    | ((data[offset + i_4 + 2] & 0xff) << 16)
			    | ((data[offset + i_4 + 3] & 0xff) << 24);
			
			// mix functions
		     k *= C1_32;
		     k = Integer.rotateLeft(k, R1_32);
		     k *= C2_32;
		     hash ^= k;
		     hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;	
		}
		
	    // tail
	    int idx = nBlocks << 2;
	    int k1 = 0;
	    switch (stringLength - idx) {
	      case 3:
	        k1 ^= data[offset + idx + 2] << 16;
	      case 2:
	        k1 ^= data[offset + idx + 1] << 8;
	      case 1:
	        k1 ^= data[offset + idx];

	        // mix functions
	        k1 *= C1_32;
	        k1 = Integer.rotateLeft(k1, R1_32);
	        k1 *= C2_32;
	        hash ^= k1;
	    }

	    // finalization
	    hash ^= stringLength;
	    hash ^= (hash >>> 16);
	    hash *= 0x85ebca6b;
	    hash ^= (hash >>> 13);
	    hash *= 0xc2b2ae35;
	    hash ^= (hash >>> 16);

	    return hash & (length -1);	
	
	}
	
	



}