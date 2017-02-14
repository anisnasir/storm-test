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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
	int numItems; //unique Elements
	double serviceTimeSkew;
	double inputSkew;
	ZipfDistribution serviceTimeZipf;
	ZipfDistribution inputZipf;
	ArrayList<LocalObject> list;
	String randomStr;
	int messageCount;
	long msgId = 0;

	private class LocalObject {
		String word;
		Long processingTime;
		LocalObject(String word, long time) {
			this.word = word;
			processingTime = time;
		}
	}
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		numItems = 1000;
		serviceTimeSkew = 1.0;
		inputSkew = 1.0;
		serviceTimeZipf = new ZipfDistribution(numItems,serviceTimeSkew);
		inputZipf = new ZipfDistribution(numItems,inputSkew);
		messageCount = 0;
		list = new ArrayList<LocalObject>();
		for(int i=0;i<1000;i++) {
			String word = randomString(10);
			double probability  = serviceTimeZipf.probability(i+1);
			long value  = (long)(numItems*probability);
			double inputProbability = inputZipf.probability(i+1);
			long inputValue = (long)(numItems*inputProbability);
			for(int j=0;j<inputValue;j++ )
				list.add(i, new LocalObject(word,value));
		}
		Collections.shuffle(list);

	}
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static SecureRandom rnd = new SecureRandom();

	String randomString( int len ){
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	
	@Override
	public void nextTuple() {
		//if(messageCount < numMessages ) {
		LocalObject temp = list.get(messageCount);
		messageCount++;
		messageCount %= list.size();
		
		_collector.emit(new Values(temp.word, temp.processingTime), msgId++);
		//messageCount++;	
		//}
		//return;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "procTime"));
	}

}