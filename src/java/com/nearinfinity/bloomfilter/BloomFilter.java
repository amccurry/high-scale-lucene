/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.bloomfilter;

import java.io.IOException;
import java.io.Serializable;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.bloomfilter.bitset.BloomFilterBitSet;
import com.nearinfinity.bloomfilter.bitset.ThreadSafeBitSet;


/**
 * This is a simple implementation of a bloom filter, it uses a chain of murmur
 * hashes to create the bloom filter.
 * 
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class BloomFilter extends BloomFilterFormulas implements Serializable {
	
	private static final long serialVersionUID = -4837894658242080928L;
	private static final int seed = 1;
	
	private BloomFilterBitSet bitSet;
	private long numberOfBitsDivBy2;
	private long elementSize;
	private double probabilityOfFalsePositives;
	private int hashes;
	private int numberOfBits;
	
	public void write(IndexOutput output) throws IOException {
		output.writeLong(numberOfBitsDivBy2);
		output.writeLong(elementSize);
		output.writeLong(Double.doubleToLongBits(probabilityOfFalsePositives));
		output.writeInt(hashes);
		output.writeInt(numberOfBits);
		bitSet.write(output);
	}
	
	public void read(IndexInput input) throws IOException {
		numberOfBitsDivBy2 = input.readLong();
		elementSize = input.readLong();
		probabilityOfFalsePositives = Double.longBitsToDouble(input.readLong());
		hashes = input.readInt();
		numberOfBits = input.readInt();
		bitSet = new ThreadSafeBitSet();
		bitSet.read(input);
	}
	
	public BloomFilter() {
		//do nothing
	}
	
	/**
	 * Creates a bloom filter with the provided number of hashed and hits.
	 * @param probabilityOfFalsePositives  the probability of false positives for the given number of elements.
	 * @param numberOfBits the numberOfBits to be used in the bit set.
	 */
	public BloomFilter(double probabilityOfFalsePositives, long elementSize) {
		this.hashes = getOptimalNumberOfHashesByBits(elementSize, getNumberOfBits(probabilityOfFalsePositives, elementSize));
		this.numberOfBits = getNumberOfBits(probabilityOfFalsePositives, elementSize);
		this.numberOfBitsDivBy2 = numberOfBits / 2;
		this.bitSet = new ThreadSafeBitSet(numberOfBits);
		this.probabilityOfFalsePositives = probabilityOfFalsePositives;
		this.elementSize = elementSize;
	}
	
	/**
	 * Add a key to the bloom filter.
	 * @param key the key.
	 */
	public void addBytes(byte[] key, int offset, int length) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, offset, length);
			setBitSet(hash);
			bs[0]++;
		}
		bs[0] -= hashes; //reset to original value
	}

	/**
	 * Tests a key in the bloom filter, it may provide false positives.
	 * @param key the key.
	 * @return boolean.
	 */
	public boolean testBytes(byte[] key, int offset, int length) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, offset, length);
			if (!testBitSet(hash)) {
				bs[0] -= i; //reset to original value
				return false;
			}
			bs[0]++;
		}
		bs[0] -= hashes; //reset to original value
		return true;
	}
	
	/**
	 * Gets the number of long words in the bit set.
	 * @return the number of bytes in the heap (not counting jvm overhead).
	 */
	public long getMemorySize() {
		return bitSet.getMemorySize();
	}

	/**
	 * Sets the bit position in the bit set.
	 * @param hash the hash produced by the murmur class.
	 */
	private void setBitSet(int hash) {
		bitSet.set(getIndex(hash));
	}
	
	/**
	 * Tests the bit position in the bit set.
	 * @param hash the hash produced by the murmur class.
	 * @return boolean.
	 */
	private boolean testBitSet(int hash) {
		return bitSet.get(getIndex(hash));
	}
	
	/**
	 * Gets the index into the bit set for the given hash.
	 * @param hash the hash produced by the murmur class.
	 * @return the index position.
	 */
	private long getIndex(int hash) {
		return (hash % numberOfBitsDivBy2) + numberOfBitsDivBy2;
	}

}
