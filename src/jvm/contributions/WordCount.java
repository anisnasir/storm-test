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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount implements IRichBolt {
	public static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
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

	public void testWait(long INTERVAL){
		long start = System.nanoTime();
		long end=0;
		do{
			end = System.nanoTime();
		}while(start + INTERVAL >= end);
	}

	@Override
	public void execute(Tuple tuple) {
		String timestamp = tuple.getString(0);
		String word = tuple.getString(1);
		long processingTime = tuple.getInteger(2);
		LOG.info("tuple received " +timestamp + " " + word + " " + processingTime);
		//testWait(time*10000);
		_collector.emit(new Values(word));
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