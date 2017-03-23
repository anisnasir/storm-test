package contributions;

import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentence extends BaseBasicBolt {
	public SplitSentence() {
		//super("python", "splitsentence.py");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = tuple.getString(0);
		String delims = "[ .,?!]+";

		String words[] = sentence.split(delims);
		for(String word: words) {
			collector.emit(new Values(word));
		}
	}
}