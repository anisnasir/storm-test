package contributions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PartialKeyGrouping implements CustomStreamGrouping, Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(PartialKeyGrouping.class);
	private int numReplicas;
	private int numMessages;
	private double epsilon;
	private List<Integer> bins;
	private HashMap<Integer, LinkedList<Integer>> serverBin;
	private HashMap<Integer, Integer> binLoadMap;
	private HashMap<Integer, Integer> binWorkerMap;
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;
    private HashFunction h1 = Hashing.murmur3_128(13);

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        numReplicas = 100;
        epsilon = 0.01;
        bins = new ArrayList<Integer>();
        this.serverBin = new HashMap<Integer, LinkedList<Integer>> ();
        this.binLoadMap = new HashMap<Integer, Integer> ();
        this.binWorkerMap = new HashMap<Integer, Integer>();
        for (int node : targetTasks) {
			add(node);
		}
        this.numMessages = 0;
    }
    private void add(int node) {
		LinkedList<Integer> temp = new LinkedList<Integer>();
		for (int i = 0; i < numReplicas; i++) {
			int id = bins.size();
			temp.add(id);
			bins.add(id);
			binLoadMap.put(id, 0);
			binWorkerMap.put(id, node);
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
    		int candidateChoice =  Math.abs(h1.hashBytes(key.getBytes()).asInt()) % bins.size();
    		
    		while(binLoadMap.get(candidateChoice) >= (1+epsilon)*avgLoad) {
    			String newKey = key+":"+salt;
    			candidateChoice =  Math.abs(h1.hashBytes(newKey.getBytes()).asInt()) % bins.size();
    			salt++;
    		}
    		int currentLoad = binLoadMap.get(candidateChoice);
    		binLoadMap.put(candidateChoice, currentLoad+1);
    		int targetTask = binWorkerMap.get(candidateChoice);
    		LOG.info("target worker " + targetTask);
    		boltIds.add(targetTasks.get(targetTask));
        }
        return boltIds;
    }
}