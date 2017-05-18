package contributions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class ConsistentGroupingPoRC{
	private final int numberOfReplicas;
	List<VirtualWorker> bins;
	HashMap<Integer, LinkedList<Integer>> serverBin;
	QueueSet<Integer> underloaded;
	QueueSet<Integer> overloaded;
	int numMessages = 0;
	double epsilon;
	Hasher hash1 = new Hasher(105929);
	
	public ConsistentGroupingPoRC(List<Integer> nodes, int numberOfReplicas, double epsilon) {
		this.numberOfReplicas = numberOfReplicas;
		this.bins = new ArrayList<VirtualWorker>();
		this.underloaded = new QueueSet<Integer>();
		this.overloaded = new QueueSet<Integer>();
		this.serverBin = new HashMap<Integer, LinkedList<Integer>> ();
		for (int node : nodes) {
			add(node);
		}
		//System.out.println(epsilon);
		this.epsilon = epsilon;
	}

	public void add(int node) {
		LinkedList<Integer> temp = new LinkedList<Integer>();
		for (int i = 0; i < numberOfReplicas; i++) {
			temp.add(bins.size());
			bins.add(new VirtualWorker(0,node));
		}
		serverBin.put(node, temp);
	}
	
	public void increaseLoad(int underloadedWorker) {
		if(overloaded.isEmpty()) {
			underloaded.add(underloadedWorker);
		}else {
			int overloadedWorker = overloaded.poll();
			LinkedList<Integer> overLoadedBins = serverBin.get(overloadedWorker);
			int binIndex = overLoadedBins.getLast();
			overLoadedBins.removeLast();
			LinkedList<Integer> underLoadedBins = serverBin.get(underloadedWorker);
			underLoadedBins.add(binIndex);
			bins.get(binIndex).setWorker(underloadedWorker);
			
		}
		
	}
	
	public void reduceLoad(int overloadedWorker) {
		if(underloaded.isEmpty()) {
			overloaded.add(overloadedWorker);
		}else {
			int underloadedWorker = underloaded.poll();
			LinkedList<Integer> overLoadedBins = serverBin.get(overloadedWorker);
			int binIndex = overLoadedBins.getLast();
			overLoadedBins.removeLast();
			LinkedList<Integer> underLoadedBins = serverBin.get(underloadedWorker);
			underLoadedBins.add(binIndex);
			bins.get(binIndex).setWorker(underloadedWorker);
			
		}
	}
	
	public int getServer(byte[] item) {
		String key = item.toString();
		numMessages++;			
		int salt = 1;
		double avgLoad = numMessages/((double)bins.size());
		int candidateChoice = hash1.hash(key)%bins.size();
		VirtualWorker candidateBin = bins.get(candidateChoice);
		while(candidateBin.getLoad() >= (1+epsilon)*avgLoad) {
			String newKey = key+":"+salt;
			candidateChoice = hash1.hash(newKey)%bins.size();
			candidateBin = bins.get(candidateChoice);
			salt++;
		}
		candidateBin.incrementNumberMessage();
		return candidateBin.getWorker();
		
	} 


}