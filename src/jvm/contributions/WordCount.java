package contributions;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCount implements IRichBolt {
    Map<String, Long> counts = new HashMap<String, Long>();
    OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
	      Long count = counts.get(word);
	      if (count == null)
	        count = (long) 0;
	      count++;
	      counts.put(word, count);
	      _collector.emit(new Values(word, count));
	      _collector.ack(tuple);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this._collector = arg2;
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
  }