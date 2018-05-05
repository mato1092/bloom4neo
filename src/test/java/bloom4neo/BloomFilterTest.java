package bloom4neo;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

import bloom4neo.util.BloomFilter;

public class BloomFilterTest {

	@Test
	public void addNodeToBFTest() {
		byte[] L_in = BigInteger.ZERO.toByteArray();
		L_in = BloomFilter.addNodeToBF(12, L_in);
		assertEquals(true, BloomFilter.checkBit(12, L_in));
		assertEquals(false, BloomFilter.checkBit(3, L_in));
	}

	@Test
	public void addBFsTest() {
		// bf1: (10101)
		byte[] bf1 = BigInteger.valueOf(21).toByteArray();
		// bf2: (01010)
		byte[] bf2 = BigInteger.valueOf(10).toByteArray();
		byte[] bfResult = BloomFilter.addBFs(bf1, bf2);
		BigInteger result = new BigInteger(bfResult);
		// assert: bfResult == (11111)
		assertEquals(true, result.compareTo(BigInteger.valueOf(31)) == 0);
		// bf1: (11111)
		bf1 = BigInteger.valueOf(31).toByteArray();
		// bf2: (01010)
		bf2 = BigInteger.valueOf(10).toByteArray();
		bfResult = BloomFilter.addBFs(bf1, bf2);
		result = new BigInteger(bfResult);
		// assert: bfResult == (11111)
		assertEquals(true, result.compareTo(BigInteger.valueOf(31)) == 0);
	}

}
