package contributions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PartialKeyGrouping implements CustomStreamGrouping, Serializable {
	private class VirtualWorker {
		long load;
		int worker;
		public VirtualWorker(long load, int worker) { 
			this.load = load;
			this.worker= worker;
		}
		public long getLoad() {
			return load;
		}
		public void setLoad(long load) {
			this.load = load;
		}
		public int getWorker() {
			return worker;
		}
		public void setWorker(int worker) {
			this.worker = worker;
		}
		public void incrementNumberMessage() {
			load++;
		}
	}
	private int numReplicas;
	private int numMessages;
	private double epsilon;
	List<VirtualWorker> bins;
	HashMap<Integer, LinkedList<Integer>> serverBin;
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;
    private long[] targetTaskStats;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        targetTaskStats = new long[this.targetTasks.size()];
        numReplicas = 100;
        epsilon = 0.01;
        this.bins = new ArrayList<VirtualWorker>();
        this.serverBin = new HashMap<Integer, LinkedList<Integer>> ();
        for (int node : targetTasks) {
			add(node);
		}
        this.numMessages = 0;
    }
    private void add(int node) {
		LinkedList<Integer> temp = new LinkedList<Integer>();
		for (int i = 0; i < numReplicas; i++) {
			temp.add(bins.size());
			bins.add(new VirtualWorker(0,node));
		}
		serverBin.put(node, temp);
	}

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if (values.size() > 0) {
        	String key = values.get(0).toString();
    		numMessages++;			
    		int salt = 1;
    		double avgLoad = numMessages/((double)bins.size());
    		int candidateChoice = (int) (Math.abs(h1.hashBytes(key.getBytes()).asLong()) % this.targetTasks.size());
    		VirtualWorker candidateBin = bins.get(candidateChoice);
    		while(candidateBin.getLoad() >= (1+epsilon)*avgLoad) {
    			String newKey = key+":"+salt;
    			candidateChoice = (int) (Math.abs(h1.hashBytes(newKey.getBytes()).asLong()) % this.targetTasks.size());
    			candidateBin = bins.get(candidateChoice);
    			salt++;
    		}
    		candidateBin.incrementNumberMessage();
    		boltIds.add(targetTasks.get(candidateBin.getWorker()));
        }
        return boltIds;
    }
}