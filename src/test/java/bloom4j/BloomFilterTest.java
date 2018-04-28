package bloom4j;

import static org.junit.Assert.*;

import org.junit.Test;

import util.BloomFilter;

public class BloomFilterTest {

	@Test
	public void basicFunctionalityExistsTest() {
		BloomFilter filter = new BloomFilter();
		String L_in = "00000000";
		L_in = filter.add("12", L_in);
		assertEquals(true, filter.check("12", L_in));
		
	}
	
	@Test
	public void basicFunctionalityDontExistsTest() {
		BloomFilter filter = new BloomFilter();
		String L_in = "00000000";
		L_in = filter.add("12", L_in);
		assertEquals(false, filter.check("3", L_in));
		
	}

}
