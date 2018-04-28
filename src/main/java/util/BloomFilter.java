package util;

public class BloomFilter {
	
	int hash = 8;
	
	/*
	 * First implementation of the bloom filter with MOD
	 * */
	public BloomFilter() {
		
	}

	public String add(String n, String value) {
		int index = this.getIndex(n);
		StringBuilder result = new StringBuilder(value);
		result.setCharAt(index, '1');
		return result.toString();
	}

	public boolean check(String n, String value) {
		int index = this.getIndex(n);

		if (value.charAt(index) == '1') return true;
		return false;
	}
	
	private int getIndex(String n) {
		//todo here doing the real magic
		return Integer.parseInt(n) % this.hash;
	}
	
	

}
