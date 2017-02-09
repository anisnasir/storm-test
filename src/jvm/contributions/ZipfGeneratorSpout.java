/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package contributions;

import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ZipfGeneratorSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	int numMessages;
	int k; //unique Elements
	double skew;
	ZipfDistribution zipf;
	String randomStr;
	int messageCount;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		numMessages = 41666;
		k = 10000;
		skew = 1.0;
		zipf = new ZipfDistribution(k,skew);
		messageCount = 0;

	}
	@Override
	public void nextTuple() {
		if(messageCount < numMessages ) {
			long num = zipf.sample();
			String sentence = String.valueOf(num);
			_collector.emit(new Values(sentence));
			messageCount++;	
		}
		return;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}