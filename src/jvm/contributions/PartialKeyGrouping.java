package contributions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.FastMath;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.commons.math3.util.FastMath;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class PartialKeyGrouping implements CustomStreamGrouping,Serializable{
	private List<Integer> targetTasks;
	private  long[] targetTaskStats;
	WorkerTopologyContext context;
	private HashFunction h1 = Hashing.murmur3_128(13);
	private HashFunction h2 = Hashing.murmur3_128(17);

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		this.context = context;
		this.targetTasks = targetTasks;
		targetTaskStats = new long[this.targetTasks.size()];
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList();
		if(values.size()>0)
		{
			String str = values.get(0).toString();
			Long timeStamp = Long.parseLong(values.get(1).toString());
			if(str.isEmpty())
				boltIds.add(targetTasks.get(0));
			else
			{
				int firstChoice = (int) (FastMath.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
				int secondChoice = (int) (FastMath.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
				int selected = targetTaskStats[firstChoice]>targetTaskStats[secondChoice]?secondChoice:firstChoice;

				boltIds.add(targetTasks.get(selected));
				targetTaskStats[selected]++;
			}

		}
		return boltIds;
	}

}
