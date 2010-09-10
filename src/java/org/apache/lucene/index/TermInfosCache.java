package org.apache.lucene.index;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.store.Directory;

import com.nearinfinity.bloomfilter.BloomFilter;

public class TermInfosCache {
	
	private static Map<Directory, Map<String,BloomFilter>> bloomFilterCache = new WeakHashMap<Directory, Map<String,BloomFilter>>();
	
	public synchronized static void addToCache(Directory directory, String segment, BloomFilter bloomFilter) {
		Map<String, BloomFilter> map = bloomFilterCache.get(directory);
		if (map == null) {
			map = new WeakHashMap<String, BloomFilter>();
			bloomFilterCache.put(directory, map);
		}
		map.put(segment, bloomFilter);
	}
	
	public static BloomFilter getFromCache(Directory directory, String segment) {
		Map<String, BloomFilter> map = bloomFilterCache.get(directory);
		if (map == null) {
			return null;
		}
		return map.get(segment);
	}
}
