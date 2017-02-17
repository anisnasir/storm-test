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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Lists;

import org.apache.storm.grouping.LoadMapping;

public class RelaxedConsistentGroupingTest {
    @Test
    public void testChooseTasks() {
    	RelaxedConsistentGrouping grouping = new RelaxedConsistentGrouping();
    	LoadMapping loadMapping = new LoadMapping();
    	List<Integer> targetTasks = Lists.newArrayList(0, 1, 2, 3, 4, 5,6,7,8,9);
    	Map<Integer, Double> local = new HashMap<Integer,Double>();
    	for(int i = 0; i< 10;i++) {
    		local.put(i, 0.5);
    	}
    	loadMapping.setLocal(local);
    	grouping.prepare(null, null, targetTasks);
        Values message1 = new Values("key1");
        List<Integer> choice1 = grouping.chooseTasks(0, message1, loadMapping);
        assertThat(choice1.size(), is(1));
        List<Integer> choice2 = grouping.chooseTasks(0, message1,loadMapping);
        assertThat(choice2, is(choice1));
        Values message2 = new Values("1232");
        //List<Integer> choice3 = grouping.chooseTasks(0, message2, loadMapping);
        //assertThat(choice3.get(0), not(is(choice1.get(0)))); 
    }
}
