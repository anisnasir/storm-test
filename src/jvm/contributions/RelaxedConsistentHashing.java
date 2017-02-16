package contributions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;



public class RelaxedConsistentHashing{
	private HashFunction hashFunction = Hashing.murmur3_128(13);
	private final int numWorkers; 
	private final int numReplicas;
	private final SortedMap<Integer, Integer> circle =
			new TreeMap<Integer, Integer>();
	private final HashMap<Integer, Integer> numVirtualServers = 
			new HashMap<Integer, Integer>();

	public RelaxedConsistentHashing(int numberOfWorkers, int numberOfReplicas) {
		this.numWorkers = numberOfWorkers;
		this.numReplicas = numberOfReplicas;

		Collection<Integer> nodes = new ArrayList<Integer>();
		for(int i = 0 ; i< numWorkers;i++) 
			nodes.add(i);
		
		for (int node : nodes) {
			add(node);
		}
	}

	public void add(int node) {
		for (int i = 0; i < numReplicas; i++) {
			String str = node +":"+ i;
			int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
			circle.put(key,
					node);
		}
		numVirtualServers.put(node, numReplicas);
		
	}
	
	public void remove(int node) {
		for (int i = 0; i < numVirtualServers.get(node); i++) {
			String str = node +":"+ i;
			int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
			circle.remove(key);
		}
		numVirtualServers.remove(node);
	}

	public int getServer(byte[] raw) {
		if (circle.isEmpty()) {
			return -1;
		}
		int hash = (int) (Math.abs(hashFunction.hashBytes(raw).asLong()));
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, Integer> tailMap =
					circle.tailMap(hash);
			hash = tailMap.isEmpty() ?
					circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
	
	public void increaseLoad(int node) {
		int replicaCount = numVirtualServers.get(node);		
		String str = node +":"+ replicaCount;
		int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
		circle.put(key,node);
		numVirtualServers.put(node, replicaCount+1);
				
	}
	
	public void reduceLoad(int node) {
		int replicaCount = numVirtualServers.get(node);		
		if(replicaCount > 0) {
			String str = node +":"+ (replicaCount-1);
			int key = (int) (Math.abs(hashFunction.hashBytes(str.getBytes()).asLong()));
			circle.remove(key);
			numVirtualServers.put(node, replicaCount-1);
		}
	}


}