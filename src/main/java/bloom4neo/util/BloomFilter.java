package bloom4neo.util;

public class BloomFilter {

	int hash = 8;

	/*
	 * First implementation of the bloom filter with MOD
	 * */
	public BloomFilter() {

	}

	/**
	 * author mri
	 * @param n element which will be hashed
	 * @param value current value of bloomfilter
	 * @return new value of bloomfilter after adding n
	 */
	public String add(String n, String value) {
		//todo handle it
		if(value.equals("")){
			value = "00000000";
		}
		int index = this.getIndex(n);
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
		//todo here doing the real magic
		return Integer.parseInt(n) % this.hash;
	}



}