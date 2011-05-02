/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nexr.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.FlumeMaster;
import com.nexr.agent.cp.CheckPointManager;
import com.nexr.agent.cp.CheckPointManagerImpl;

/**
 * Test cases for the LivenessManager module.
 */
public class TestCheckPointManager {

//	FlumeMaster master = null;
//
//	FlumeConfiguration cfg;
//
//	CheckPointManager manager;
//	@Before
//	public void setCfg() throws IOException {
////		CheckPointAgentManager manager = new CheckPointAgentManagerImpl();
////		manager.startServer();
////		manager.startClient("localhost");
//	}
//
//	@After
//	public void shutdownMaster() {
//		// if (master != null) {
//		// master.shutdown();
//		// master = null;
//		// }
//	}
//
//	@Test
//	public void testGetTagID() {
//		System.out.println(manager.getTagId("agent1", "tx.log"));
//		String tagId = manager.getTagId("agent1", "tx.log");
//		assertEquals("", tagId);
//	}
//
//	@Test
//	public void testAddPendingQ() {
//		manager = new CheckPointManagerImpl();
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("tx.log", 100L);
//		manager.addPendingQ("agent1_tx.log_00000001.20110408-121846312+0900.185924549635029", content1);
//	}
//	
//	@Test
//	public void testCheckPointFile() {
//		CheckPointManagerImpl manager = new CheckPointManagerImpl();
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("tx.log", 3L);
//		manager.addPendingQ("agent1_tx.log_00000001.20110408-121846312+0900.185924549635030", content1);
//		
//		List<String> tagIds = new ArrayList<String>();
//		tagIds.add("agent1_tx.log_00000001.20110408-121846312+0900.185924549635030");
//		manager.updateCheckPointFile("agent1", tagIds);
//	}
//	
//	@Test
//	public void testCheckPointFiles() {
//		CheckPointManagerImpl manager = new CheckPointManagerImpl();
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("tx.log", 1010L);
//		Map<String, Long> content2 = new HashMap<String, Long>();
//		content2.put("debug.log", 1112L);
//		Map<String, Long> content3 = new HashMap<String, Long>();
//		content3.put("error.log", 123L);
//		
//		Map<String, Long> content4 = new HashMap<String, Long>();
//		content4.put("error.log", 13123L);
//		
//		manager.addPendingQ("agent1_tx.log_00000001.20110408-121846312+0900.185924549635030", content1);
//		manager.addPendingQ("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030", content2);
//		manager.addPendingQ("agent1_error.log_00000001.20110408-121846312+0900.185924549635030", content3);
//		
//		manager.addPendingQ("agent2_error.log_00000001.20110408-121846312+0900.185924549635031", content4);
//
//		List<String> tagIds = new ArrayList<String>();
//		List<String> tagIds2 = new ArrayList<String>();
//		
//		tagIds.add("agent1_tx.log_00000001.20110408-121846312+0900.185924549635030");
//		tagIds.add("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030");
//		tagIds.add("agent1_error.log_00000001.20110408-121846312+0900.185924549635030");
//
//		tagIds2.add("agent2_error.log_00000001.20110408-121846312+0900.185924549635031");
//		
//		manager.updateCheckPointFile("agent1", tagIds);
//		manager.updateCheckPointFile("agent2", tagIds2);
//	}
//	
//	
//	@Test
//	public void testCheckPointFiles2() {
//		CheckPointManagerImpl manager = new CheckPointManagerImpl();
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("debug.log", 1010L);
//		Map<String, Long> content2 = new HashMap<String, Long>();
//		content2.put("debug.log", 1112L);
//		Map<String, Long> content3 = new HashMap<String, Long>();
//		content3.put("debug.log", 123L);
//		
//		manager.addPendingQ("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030", content1);
//		manager.addPendingQ("agent1_debug.log_00000001.20110408-121846312+0900.185924549635036", content2);
//		manager.addPendingQ("agent1_debug.log_00000001.20110408-121846312+0900.185924549635034", content3);
//		
//
//		List<String> tagIds = new ArrayList<String>();
//		
//		tagIds.add("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030");
//		tagIds.add("agent1_debug.log_00000001.20110408-121846312+0900.185924549635036");
//		tagIds.add("agent1_debug.log_00000001.20110408-121846312+0900.185924549635034");
//
//		
//		manager.updateCheckPointFile("agent1", tagIds);
//	}
//	
//	@Test
//	public void testGetOffset() {
//		CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
//		
//		Map map = manager.getOffset("agent1");
//		
//		Set<String> keySet = map.keySet();
//		Object[] keys = keySet.toArray();
//		for(int i=0; i<keys.length; i++){
//			System.out.println(keys[i].toString() + " " + map.get(keys[i].toString()));
//		}
//	}
//	
//	@Test
//	public void testAddCollectorPendingList() {
//		CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
//		manager.addCollectorPendingList("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030");
//		manager.addCollectorPendingList("agent1_debug.log_00000001.20110408-121846312+0900.185924549635036");
//		manager.addCollectorPendingList("agent1_tx.log_00000001.20110408-121846312+0900.185924549635034");
//	}
//	
//	@Test
//	public void testMoveToCompleteList() {
//		CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
//		manager.addCollectorPendingList("agent1_debug.log_00000001.20110408-121846312+0900.185924549635030");
//		manager.addCollectorPendingList("agent1_debug.log_00000001.20110408-121846312+0900.185924549635036");
//		manager.addCollectorPendingList("agent1_tx.log_00000001.20110408-121846312+0900.185924549635034");
//		
//		manager.moveToCompleteList();
//		
//	}
//	
//	@Test
//	public void testStartServer() {
//		CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
//		manager.startServer();
//	}
//	
//	@Test
//	public void testGetTagList() {
//		//total api test
//		CheckPointManager manager = FlumeNode.getInstance().getCheckPointManager();
//		
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("debug.log", 1010L);
//		Map<String, Long> content2 = new HashMap<String, Long>();
//		content2.put("debug.log", 1112L);
//		Map<String, Long> content3 = new HashMap<String, Long>();
//		content3.put("debug.log", 123L);
//		String tagId1 = manager.getTagId("agent1", "debug.log");
//		String tagId2 = manager.getTagId("agent1", "debug.log");
//		String tagId3 = manager.getTagId("agent1", "debug.log");
//		
//		manager.addPendingQ(tagId1, content1);
//		manager.addPendingQ(tagId2, content2);
//		manager.addPendingQ(tagId3, content3);
//		
//	
//		//Collector
//		manager.addCollectorPendingList(tagId1);
//		manager.addCollectorPendingList(tagId2);
//		manager.addCollectorPendingList(tagId3);
//		
//		
////		manager.startClient("localhost");
//		manager.setCollectorHost("localhost");
//		manager.startTagChecker();
//		manager.moveToCompleteList();
//		manager.startServer();
//		
//		
//		
//	}
//	
//	
//	@Test
//	public void testGetTagList2() {
//		//total api test
//		CheckPointManager manager =FlumeNode.getInstance().getCheckPointManager();
//		
//		Map<String, Long> content1 = new HashMap<String, Long>();
//		content1.put("tx.log", 1010L);
//		Map<String, Long> content2 = new HashMap<String, Long>();
//		content2.put("tx.log", 1112L);
//		Map<String, Long> content3 = new HashMap<String, Long>();
//		content3.put("tx.log", 123L);
//		Map<String, Long> content4 = new HashMap<String, Long>();
//		content3.put("debug.log", 11123L);
//		
//		String tagId1 = manager.getTagId("agent1", "tx.log");
//		String tagId2 = manager.getTagId("agent1", "tx.log");
//		String tagId3 = manager.getTagId("agent1", "tx.log");
//		String tagId4 = manager.getTagId("agent1", "debug.log");
//		
//		manager.addPendingQ(tagId1, content1);
//		manager.addPendingQ(tagId2, content2);
//		manager.addPendingQ(tagId3, content3);
//		manager.addPendingQ(tagId4, content4);
//	
//		//Collector
//		manager.addCollectorPendingList(tagId1);
//		manager.addCollectorPendingList(tagId2);
//		manager.addCollectorPendingList(tagId3);
//		manager.addCollectorPendingList(tagId4);
//		
//		
////		manager.startClient("localhost");
//		manager.setCollectorHost("localhost");
//		manager.startTagChecker();
//		manager.moveToCompleteList();
//		manager.startServer();
//		
//		
//	}
}
