/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.bloomfilter.bitset;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * A thread safe implementation of a bit set.  Uses {@link AtomicLongArray} for
 * thread safety, by using compare and swap.
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class ThreadSafeBitSet extends BloomFilterBitSet implements Cloneable, Serializable {
	private static final long serialVersionUID = -7595377654535919085L;
	
	private AtomicLongArray bits;
	
	public ThreadSafeBitSet() {
		
	}

	public ThreadSafeBitSet(long numBits) {
		this.bits = new AtomicLongArray(bits2words(numBits));
	}

	public ThreadSafeBitSet(long[] bits) {
		this.bits = new AtomicLongArray(bits);
	}

	@Override
	public boolean get(long index) {
		int i = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		long currentWord = bits.get(i);
		return (currentWord & bitmask) != 0;
	}

	@Override
	public void set(long index) {
		int wordNum = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		long currentWord = bits.get(wordNum);
		long newWord = currentWord | bitmask;
		while (!bits.compareAndSet(wordNum, currentWord, newWord)) {
			currentWord = bits.get(wordNum);
			newWord = currentWord | bitmask;
		}
	}

	@Override
	public long getMemorySize() {
		return (long) bits.length() * 8L;
	}
	
	public static int bits2words(long numBits) {
		return (int) (((numBits - 1) >>> 6) + 1);
	}

	@Override
	public void read(IndexInput input) throws IOException {
		int length = input.readInt();
		bits = new AtomicLongArray(length);
		for (int i = 0; i < length; i++) {
			bits.set(i, input.readLong());
		}
	}

	@Override
	public void write(IndexOutput output) throws IOException {
		int length = bits.length();
		output.writeInt(length);
		for (int i = 0; i < length; i++) {
			output.writeLong(bits.get(i));
		}
	}

}
