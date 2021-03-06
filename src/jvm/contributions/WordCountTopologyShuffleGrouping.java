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


import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopologyShuffleGrouping {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    
    String zkConnString="9.116.35.208:2181";
    String topicName= "wiki";
    BrokerHosts hosts = new ZkHosts(zkConnString);
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
    //spoutConfig.scheme = new SchemeAsMultiScheme(new WikiScheme());
    spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new WikiScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    builder.setSpout("spout", kafkaSpout, 1);

    //builder.setBolt("split", new SplitSentence(), 8).fieldsGrouping("spout", new Fields("word"));
    //builder.setBolt("count", new WordCount(), 12).fieldsGrouping("spout", new Fields("word"));
    builder.setBolt("count", new WordCount(), 15).shuffleGrouping("spout");

    
    Config conf = new Config();
    //conf.setDebug(true);
    conf.setMaxSpoutPending(1000);
    //conf.put(Config.TOPOLOGY_DEBUG, true);
    //conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
    //conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
    //conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
    conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);

    if (args != null && args.length > 0) {
    	conf.setNumWorkers(16); // use two worker processes
    	

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
