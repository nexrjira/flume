package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.print.DocFlavor.STRING;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;
import com.nexr.collector.cp.CheckPointHandler;
import com.nexr.cp.thrift.CheckPointService;

public class CheckPointManagerImpl implements CheckPointManager {
	static final Logger log = LoggerFactory
			.getLogger(CheckPointManagerImpl.class);

	private final static String DATE_FORMAT = "yyyyMMdd-HHmmssSSSZ";
	private final String SEPERATOR = "\t";
	private final String LINE_SEPERATOR = "\n";

	private String checkPointFilePath;

	private Map<String, TTransport> agentTransportMap; // agent-collector
														// TTransport
														// mappingInfo
	private Map<String, CheckPointService.Client> agentClientMap; // agent-collector
																	// Client
																	// mappingInfo
	private Map<String, CollectorInfo> agentCollectorInfo;

	private Map<String, List<PendingQueueModel>> agentTagMap; // agent,
																// list<PendingQueueModel>
	private Map<String, WaitingQueueModel> waitedTagList; // agent,
															// WatingQueueModel

	private Object sync = new Object();

	CheckTagIDThread checkTagIdThread;
	ServerThread serverThread;
	ClientThread clientThread;

	String collectorHost;

	List<String> agentList;

	int timeout = 10 * 1000;

	// for collector
	private final List<String> pendingList;
	private final List<String> completeList;

	CheckPointService.Processor processor;
	TNonblockingServerSocket serverSocket;
	TNonblockingServer.Args arguments;
	TServer server;

	private boolean clientStarted = false;

	public CheckPointManagerImpl() {
		// agentList = new ArrayList<String>();
		// agentTagMap = new HashMap<String, List<PendingQueueModel>>();
		// waitedTagList = new HashMap<String, WaitingQueueModel>();
		// checkPointFilePath = FlumeConfiguration.get().getCheckPointFile();
		// checkTagIdThread = new CheckTagIDThread();
		// pendingList = new ArrayList<String>();
		// completeList = new ArrayList<String>();
		// agentTransportMap = new HashMap<String, TTransport>();
		// agentClientMap = new HashMap<String, CheckPointService.Client>();
		// agentCollectorInfo = new HashMap<String, CollectorInfo>();

		agentList = Collections.synchronizedList(new ArrayList<String>());
		agentTagMap = Collections
				.synchronizedMap(new HashMap<String, List<PendingQueueModel>>());
		waitedTagList = Collections
				.synchronizedMap(new HashMap<String, WaitingQueueModel>());
		checkPointFilePath = FlumeConfiguration.get().getCheckPointFile();
		checkTagIdThread = new CheckTagIDThread();
		pendingList = Collections.synchronizedList(new ArrayList<String>());
		completeList = Collections.synchronizedList(new ArrayList<String>());
		agentTransportMap = Collections
				.synchronizedMap(new HashMap<String, TTransport>());
		agentClientMap = Collections
				.synchronizedMap(new HashMap<String, CheckPointService.Client>());
		agentCollectorInfo = Collections
				.synchronizedMap(new HashMap<String, CollectorInfo>());
	}

	class ClientThread extends Thread {
		volatile boolean done = false;
		long checkTagIdPeriod = FlumeConfiguration.get()
				.getConfigHeartbeatPeriod();
		CountDownLatch stopped = new CountDownLatch(1);

		ClientThread() {
			super("CheckManager Client");
		}

		TSocket socket = null;
		TTransport transport = null;
		TProtocol protocol = null;
		CheckPointService.Client client = null;

		public void run() {
			log.info("Done " + done + " AgentSize " + agentList.size());
			while (!done) {
				synchronized (sync) {
					if (agentClientMap.size() == agentCollectorInfo.size()) {

					} else {
						for (int i = 0; i < agentList.size(); i++) {
							if (!agentClientMap.containsKey(agentList.get(i))) {
								CollectorInfo ci = agentCollectorInfo
										.get(agentList.get(i));
								socket = new TSocket(ci.getCollectorHost(),
										ci.getCollectorPort());
								socket.setTimeout(timeout);
								transport = new TFramedTransport(socket);
								protocol = new TBinaryProtocol(transport);
								client = new CheckPointService.Client(protocol);
								log.info("New Client " + agentList.get(i)
										+ " CollectorINFO ["
										+ ci.getCollectorHost() + ":"
										+ ci.getCollectorPort() + "]");
								try {
									transport.open();

									agentTransportMap.put(agentList.get(i),
											transport);
									agentClientMap
											.put(agentList.get(i), client);
								} catch (TTransportException e) {
									// TODO Auto-generated catch block
									log.info(agentList.get(i)
											+ " Connect refuse ");
								}
							}
						}
					}
				}
				try {
					Thread.sleep(checkTagIdPeriod);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			log.info("ClientThread End; ");

			stopped.countDown();
		}
	};

	public void startClient() {
		if(!clientStarted) {
			log.info("Start client threads in CheckpointManager");
			clientThread = new ClientThread();
			clientThread.start();
			checkTagIdThread.start();
			clientStarted = true;
		}
	}

	@Override
	public void stopClient() {
		CountDownLatch stopped = clientThread.stopped;
		clientThread.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			log.error("Problem waiting for livenessManager to stop", e);
		}

		synchronized (sync) {
			agentTransportMap = new HashMap<String, TTransport>();
			agentClientMap = new HashMap<String, CheckPointService.Client>();
			agentTagMap = new HashMap<String, List<PendingQueueModel>>();
			waitedTagList = new HashMap<String, WaitingQueueModel>();
			agentList = new ArrayList<String>();
			agentCollectorInfo = new HashMap<String, CollectorInfo>();
		}
	}

	class ServerThread extends Thread {
		int port = FlumeConfiguration.get().getCheckPointPort();

		ServerThread() {
			super("CheckManager Server");
		}

		ServerThread(int port) {
			this.port = port;
		}

		public void run() {
			try {
				processor = new CheckPointService.Processor(
						new CheckPointHandler());
				serverSocket = new TNonblockingServerSocket(port);
				arguments = new TNonblockingServer.Args(serverSocket);
				arguments.processor(processor);
				server = new TNonblockingServer(arguments);
				server.serve();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	};

	@Override
	public void startServer() {	
		log.info("Start CheckpoinServer : " + FlumeConfiguration.get().getCheckPointPort());
		serverThread = new ServerThread();
		serverThread.start();
	}

	@Override
	public void startServer(int port) {
		log.info("Start Checkpoint Server : " + port);
		serverThread = new ServerThread(port);
		serverThread.start();
	}

	@Override
	public void stopServer() {
		serverThread.stop();
	}

	@Override
	public void setCollectorHost(String host) {
		this.collectorHost = host;
	}

	@Override
	public String getTagId(String agentName, String fileName) {
		// TODO Auto-generated method stub
		DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
		long pid = Thread.currentThread().getId();
		String prefix = agentName + "_" + fileName;
		Date now = new Date(Clock.unixTime());
		long nanos = Clock.nanos();
		String f;
		synchronized (dateFormat) {
			f = dateFormat.format(now);
		}
		String tagId = String.format("%s_%08d.%s.%012d", prefix, pid, f, nanos);

		return tagId;
	}

	@Override
	public Map<String, Long> getOffset(String logicalNodeName) {
		// TODO Auto-generated method stub
		// checkpoint ÌååÏùºÏóêÏÑú Ìï¥Îãπ logical NodeÏóê Ìï¥ÎãπÌïòÎäî ÌååÏùºÍ≥º
		// offsetÏùÑ Ï†ÑÎã¨.
		Map<String, Long> result = new HashMap<String, Long>();

		FileReader fileReader;
		BufferedReader reader;

		File ckpointFilePath = new File(checkPointFilePath + File.separator
				+ logicalNodeName + File.separator + "checkpoint");
		try {
			if (!ckpointFilePath.exists()) {
				return null;
			} else {
				fileReader = new FileReader(ckpointFilePath);
				reader = new BufferedReader(fileReader);
				String line = null;
				while ((line = reader.readLine()) != null) {
					result.put(line.substring(0, line.indexOf(SEPERATOR)), Long
							.valueOf(line.substring(line.indexOf(SEPERATOR),
									line.length()).trim()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	class CheckTagIDThread extends Thread {
		volatile boolean done = false;
		long checkTagIdPeriod = FlumeConfiguration.get()
				.getConfigHeartbeatPeriod();
		CountDownLatch stopped = new CountDownLatch(1);

		CheckTagIDThread() {
			super("Check TagID");
		}

		public void run() {

			while (!done) {
				try {
					checkCollectorTagID();
					Clock.sleep(checkTagIdPeriod);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			stopped.countDown();
		}

	};

	@Override
	public void startTagChecker(String agentName, String collectorHost,
			int collectorPort) {
		// startClientÎ•º Ìò∏Ï∂ú ÌïòÏßÄ ÏïäÍ≥† Ïù¥ Î©îÏÜåÎìúÎ•º Ìò∏Ï∂ú ÌïòÏó¨
		// Ïì∞Î†àÎìú ÎÇ¥ÏóêÏÑú startÎ•º Ìò∏Ï∂ú ÌïòÎèÑÎ°ù Ìï®.
		log.info("StartTagChecker [" + agentName + ", " + collectorHost + ", " + collectorPort + "]");
		startClient();
		synchronized (sync) {
			if (!agentList.contains(agentName)) {
				agentList.add(agentName);
			}

			if (!agentCollectorInfo.containsKey(agentName)) {
				agentCollectorInfo.put(agentName, new CollectorInfo(
						collectorHost, collectorPort));
			}
		}
	}

	@Override
	public void stopTagChecker(String agentName) {
		log.info("StopTagChecker [" + agentName + "]");
		synchronized (sync) {
			agentTransportMap.remove(agentName);
			agentClientMap.remove(agentName);
			agentTagMap.remove(agentName);
			waitedTagList.remove(agentName);
			agentCollectorInfo.remove(agentName);
			for (int i = 0; i < agentList.size(); i++) {
				if (agentList.get(i) == agentName) {
					agentList.remove(i);
				}
			}
		}
	}

	@Override
	public void addPendingQ(String tagId, String agentName,
			Map<String, Long> tagContent) {
		
		log.info("addpendingq : " + tagContent.size());
		for(String key : tagContent.keySet()) {
			log.info(key + " : " + tagContent.values());
		}
		
		List<PendingQueueModel> tags;
		PendingQueueModel pqm;
		synchronized (sync) {
			if (agentTagMap.containsKey(agentName)) {
				tags = agentTagMap.get(agentName);
				pqm = new PendingQueueModel(tagId, tagContent);
				tags.add(pqm);
				agentTagMap.put(agentName, tags);
			} else {
				tags = new ArrayList<PendingQueueModel>();
				pqm = new PendingQueueModel(tagId, tagContent);
				tags.add(pqm);
				agentTagMap.put(agentName, tags);
			}
			Log.info("add " + agentName + " : " + tagId + " into PendingQ");
		}
	}

	@Deprecated
	@Override
	public void addCollectorPendingList(String tagId) {
		// TODO Auto-generated method stub
		pendingList.add(tagId);
	}

	@Override
	public void addCollectorCompleteList(List<String> tagIds) {
		// TODO Auto-generated method stub
		for (int i = 0; i < tagIds.size(); i++) {
			if (!completeList.contains(tagIds.get(i))) {
				completeList.add(tagIds.get(i));
				log.info("Tag " + tagIds.get(i) + " added CompleteList");
			}
		}
		log.info("CompleteList Size " + completeList.size());
	}

	@Override
	// CollectorÏóêÏÑú Î∞îÎ°ú CompleteListÎ°ú TagIdÎ•º ÎÑ£ÏúºÎ©¥ ÌïÑÏöî ÏóÜÏùå.
	public void moveToCompleteList() {
		// TODO Auto-generated method stub
		Iterator<String> it = pendingList.iterator();
		synchronized (sync) {
			while (it.hasNext()) {
				completeList.add(it.next());
				it.remove();
			}
		}
	}

	@Override
	// Collecter tagIdÍ∞Ä ÏûàÎäîÏßÄ ÌôïÏù∏ÌïòÍ≥† ÏûàÏúºÎ©¥ TrueÎ•º Ï†ÑÎã¨ÌïòÍ≥†
	// completeListÏóêÏÑú ÏÇ≠Ï†úÌï®.
	public boolean getTagList(String tagId) {
		// TODO Auto-generated method stub
		boolean res = false;
		String v = null;
		log.info("CompleteList " + completeList.size());
		for (int i = 0; i < completeList.size(); i++) {
			if (completeList.get(i).equals(tagId)) {
				v = completeList.get(i);
				res = true;
				completeList.remove(i);
			}
		}
		log.info("CompleteTag " + v + " Result " + tagId + " " + res);
		return res;
	}

	public synchronized void checkCollectorTagID() {
		// TODO Auto-generated method stub
		// pendingQueue에 있는 agent의 tagId를 모두 체크 해보고
		// 마지막 true리턴 받은 값을 기억했다가 checkpoint파일에 update한다.
		boolean res = true;

		try {
			for (int i = 0; i < agentList.size(); i++) {
				List<PendingQueueModel> tags = agentTagMap
						.get(agentList.get(i));

				if (tags != null && agentClientMap.size() > 0) {
					List<PendingQueueModel> tmp = Collections
							.synchronizedList(new ArrayList<PendingQueueModel>());
					PendingQueueModel currentTagId = null;
					for (int t = 0; t < tags.size(); t++) {
						if (agentClientMap.get(agentList.get(i)) != null) {
							res = agentClientMap.get(agentList.get(i))
									.checkTagId(tags.get(t).getTagId());
							if (res) {
								// 현재 TagId 저장 후 리스트에서 삭제.
								currentTagId = tags.get(t);
								log.info("Current TagID "
										+ currentTagId.getTagId());
								tmp.add(tags.get(t));

							} else {
//								updateWaitingTagList(agentList.get(i), tags
//										.get(t).getTagId(), tags.get(t)
//										.getContents());
							}
						}
					}
					if (currentTagId != null) {
						updateCheckPointFile(agentList.get(i), currentTagId);
						for (int t = 0; t < tmp.size(); t++) {
							if (agentTagMap.get(agentList.get(i)) != null) {
								agentList.get(i);
								tmp.get(t);
								agentTagMap.get(agentList.get(i)).remove(
										tmp.get(t));
							}
						}
					}
				}
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void updateWaitingTagList(String agentName, String tagId,
			Map<String, Long> contents) {
		Set<String> keySet = waitedTagList.keySet();
		Object[] keys = keySet.toArray();
		for (int i = 0; i < keys.length; i++) {
			if (waitedTagList.get(keys[i]).getWaitedTime() >= FlumeConfiguration
					.get().getCheckPointTimeout()) {
				PendingQueueModel pqm = new PendingQueueModel(waitedTagList
						.get(keys[i]).getTagId(), waitedTagList.get(keys[i])
						.getContents());
				updateCheckPointFile(keys[i].toString(), pqm);

				// waitedTagListÏóêÏÑú ÏÇ≠Ï†ú
				waitedTagList.remove(keys[i]);
				agentTagMap.remove(agentName);
			}
		}

		WaitingQueueModel wqm = null;
		if (waitedTagList.containsKey(agentName)) {
			wqm = waitedTagList.get(agentName);
			wqm.updateWaitedTime(FlumeConfiguration.get()
					.getConfigHeartbeatPeriod());
			waitedTagList.put(agentName, wqm);
		} else {
			wqm = new WaitingQueueModel(tagId, contents, 0);
			waitedTagList.put(agentName, wqm);
		}

	}

	public void updateCheckPointFile(String logicalNodeName,
			PendingQueueModel pendingQueueModel) {
		// TODO Auto-generated method stub
		Map<String, Long> res = pendingQueueModel.getContents();
		Set<String> keySet = res.keySet();
		Object[] keys = keySet.toArray();

		File ckpointFilePath = new File(checkPointFilePath + File.separator
				+ logicalNodeName);

		File ckpointFile = new File(checkPointFilePath + File.separator
				+ logicalNodeName + File.separator + "checkpoint");
		Log.info("CheckPoint File Path " + ckpointFile.toString());
		FileReader fileReader;
		BufferedReader reader;
		FileWriter fw;
		BufferedWriter bw = null;
		StringBuilder contents;

		Map<String, String> compareMap = new HashMap<String, String>();

		try {
			if (!ckpointFilePath.exists()) {
				ckpointFilePath.mkdirs();
				ckpointFile.createNewFile();
			}

			log.info("[" + ckpointFile.getPath() + "]"
					+ " Check Point File Size " + ckpointFile.length());

			String line = null;
			if (ckpointFile.length() > 1) {
				contents = new StringBuilder();
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);

				// 현재 체크포인트 파일을 읽어서 메모리에 저장.
				while ((line = reader.readLine()) != null) {
					compareMap.put(
							line.substring(0, line.indexOf(SEPERATOR)).trim(),
							line.substring(line.indexOf(SEPERATOR),
									line.length()).trim());
				}

				// 입력 받은 TagID의 값 입력
				for (int i = 0; i < keys.length; i++) {
					compareMap.put(keys[i].toString(),
							String.valueOf(res.get(keys[i])));
				}

				Set cpSet = compareMap.keySet();
				Object[] cps = cpSet.toArray();
				for (int i = 0; i < cps.length; i++) {
					contents.append(cps[i].toString() + SEPERATOR
							+ compareMap.get(cps[i]) + LINE_SEPERATOR);
				}

				fw = new FileWriter(ckpointFile);
				bw = new BufferedWriter(fw);
				bw.write(contents.toString());
				log.info("content is : " + contents);
//				bw.close();

			} else {
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);
				contents = new StringBuilder();

				for (int i = 0; i < keys.length; i++) {
					contents.append(keys[i].toString() + SEPERATOR
							+ res.get(keys[i]) + LINE_SEPERATOR);
				}
				fw = new FileWriter(ckpointFile);
				bw = new BufferedWriter(fw);
				bw.write(contents.toString());
				log.info("content is : " + contents);
//				bw.close();
				reader.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bw.flush();
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws InterruptedException {
		CheckPointManager cp = FlumeNode.getInstance().getCheckPointManager();

		// 1. Agent Retry Î∞è ÏÑúÎ≤Ñ Ïó∞Í≤∞ ÌÖåÏä§Ìä∏
		/*
		 * cp.startClient(); cp.startTagChecker("agent1", "localhost", 9999);
		 * cp.startTagChecker("agent2", "localhost", 13421); Thread.sleep(2000);
		 * cp.startServer();
		 */

		// 2. Agent, Collector Ïó∞Í≤∞
		/*
		 * cp.startClient(); cp.startTagChecker("agent1", "localhost", 13421);
		 * Thread.sleep(2000); cp.startServer();
		 * 
		 * Map<String, Long> content1 = new HashMap<String, Long>();
		 * content1.put("tx.log", 1010L); Map<String, Long> content2 = new
		 * HashMap<String, Long>(); content2.put("tx.log", 1112L); Map<String,
		 * Long> content3 = new HashMap<String, Long>(); content3.put("tx.log",
		 * 123L); Map<String, Long> content4 = new HashMap<String, Long>();
		 * content3.put("debug.log", 11123L);
		 * 
		 * List<String> tagIds = new ArrayList<String>(); String tagId1 =
		 * cp.getTagId("agent1", "tx.log"); String tagId2 =
		 * cp.getTagId("agent1", "tx.log"); String tagId3 =
		 * cp.getTagId("agent1", "tx.log"); String tagId4 =
		 * cp.getTagId("agent1", "debug.log");
		 * 
		 * cp.addPendingQ(tagId1, "agent1", content1); cp.addPendingQ(tagId2,
		 * "agent1",content2); cp.addPendingQ(tagId3, "agent1",content3);
		 * cp.addPendingQ(tagId4, "agent1",content4);
		 * 
		 * tagIds.add(tagId1);tagIds.add(tagId2);tagIds.add(tagId3);tagIds.add(
		 * tagId4); cp.addCollectorCompleteList(tagIds);
		 */

		// 2. Agent, Collector Ïó∞Í≤∞
		/*
		 * cp.startClient(); cp.startTagChecker("agent1", "localhost", 13421);
		 * cp.startTagChecker("agent2", "localhost", 13421); Thread.sleep(2000);
		 * cp.startServer();
		 * 
		 * Map<String, Long> content1 = new HashMap<String, Long>();
		 * content1.put("tx.log", 1010L); Map<String, Long> content2 = new
		 * HashMap<String, Long>(); content2.put("tx.log", 1112L); Map<String,
		 * Long> content3 = new HashMap<String, Long>(); content3.put("tx.log",
		 * 123L); Map<String, Long> content4 = new HashMap<String, Long>();
		 * content4.put("debug.log", 11123L);
		 * 
		 * List<String> tagIds = new ArrayList<String>(); String tagId1 =
		 * cp.getTagId("agent1", "tx.log"); String tagId2 =
		 * cp.getTagId("agent2", "tx.log"); String tagId3 =
		 * cp.getTagId("agent1", "tx.log"); String tagId4 =
		 * cp.getTagId("agent2", "debug.log");
		 * 
		 * cp.addPendingQ(tagId1, "agent1", content1); cp.addPendingQ(tagId2,
		 * "agent2",content2); cp.addPendingQ(tagId3, "agent1",content3);
		 * cp.addPendingQ(tagId4, "agent2",content4);
		 * 
		 * tagIds.add(tagId1);tagIds.add(tagId2);tagIds.add(tagId3);tagIds.add(
		 * tagId4); cp.addCollectorCompleteList(tagIds);
		 */

		// 2. Pending QueueÏóê Ïò§Îûò ÏûàÏùÑÎïå checkpointÌååÏùºÏóê Ïì∞Í≥†
		// retryÌïòÎèÑÎ°ù Ìï®.

//		cp.startClient();
		cp.startTagChecker("agent1", "localhost", 13421);
		cp.startTagChecker("agent2", "localhost", 13421);
		Thread.sleep(2000);
		cp.startServer();

		Map<String, Long> content1 = new HashMap<String, Long>();
		content1.put("tx.log", 1010L);
		Map<String, Long> content2 = new HashMap<String, Long>();
		content2.put("tx.log", 1020L);
		Map<String, Long> content3 = new HashMap<String, Long>();
		content3.put("tx.log", 1030L);
		Map<String, Long> content4 = new HashMap<String, Long>();
		content4.put("tx.log", 1040L);

		List<String> tagIds = new ArrayList<String>();
		String tagId1 = cp.getTagId("agent1", "tx.log");
		String tagId2 = cp.getTagId("agent1", "tx.log");
		String tagId3 = cp.getTagId("agent1", "tx.log");
		String tagId4 = cp.getTagId("agent1", "tx.log");

		cp.addPendingQ(tagId1, "agent1", content1);
		cp.addPendingQ(tagId2, "agent1", content2);
		cp.addPendingQ(tagId3, "agent1", content3);
		cp.addPendingQ(tagId4, "agent1", content4);

		tagIds.add(tagId1);
		tagIds.add(tagId2);
		// tagIds.add(tagId3);
		tagIds.add(tagId4);
		cp.addCollectorCompleteList(tagIds);

		Thread.sleep(20000);

		content1 = new HashMap<String, Long>();
		content1.put("tx.log", 2010L);
		content2 = new HashMap<String, Long>();
		content2.put("tx.log", 2020L);
		content3 = new HashMap<String, Long>();
		content3.put("tx.log", 2030L);
		content4 = new HashMap<String, Long>();
		content4.put("tx.log", 2040L);

		tagIds = new ArrayList<String>();
		tagId1 = cp.getTagId("agent1", "tx.log");
		tagId2 = cp.getTagId("agent2", "tx.log");
		tagId3 = cp.getTagId("agent2", "tx.log");
		tagId4 = cp.getTagId("agent1", "tx.log");

		cp.addPendingQ(tagId1, "agent1", content1);
		cp.addPendingQ(tagId2, "agent2", content2);
		cp.addPendingQ(tagId3, "agent2", content3);
		cp.addPendingQ(tagId4, "agent1", content4);

		tagIds.add(tagId1);
		tagIds.add(tagId2);
		tagIds.add(tagId3);
		tagIds.add(tagId4);
		cp.addCollectorCompleteList(tagIds);

		Thread.sleep(20000);

		content1 = new HashMap<String, Long>();
		content1.put("tx.log", 3010L);
		content2 = new HashMap<String, Long>();
		content2.put("tx.log", 3020L);
		content3 = new HashMap<String, Long>();
		content3.put("tx.log", 3030L);
		content4 = new HashMap<String, Long>();
		content4.put("tx.log", 3040L);

		tagIds = new ArrayList<String>();
		tagId1 = cp.getTagId("agent1", "tx.log");
		tagId2 = cp.getTagId("agent1", "tx.log");
		tagId3 = cp.getTagId("agent1", "tx.log");
		tagId4 = cp.getTagId("agent1", "tx.log");

		cp.addPendingQ(tagId1, "agent1", content1);
		cp.addPendingQ(tagId2, "agent1", content2);
		cp.addPendingQ(tagId3, "agent1", content3);
		cp.addPendingQ(tagId4, "agent1", content4);

		tagIds.add(tagId1);
		tagIds.add(tagId2);
		tagIds.add(tagId3);
		tagIds.add(tagId4);
		cp.addCollectorCompleteList(tagIds);
		// 2. StartTagIdChecker, StopTagIdChecker
		// cp.startClient();
		// cp.startTagChecker("agent1", "localhost", 13421);
		// Thread.sleep(2000);
		// cp.startServer();
		//
		// Map<String, Long> content1 = new HashMap<String, Long>();
		// content1.put("tx.log", 1010L);
		// Map<String, Long> content2 = new HashMap<String, Long>();
		// content2.put("tx.log", 1020L);
		// Map<String, Long> content3 = new HashMap<String, Long>();
		// content3.put("tx.log", 1030L);
		// Map<String, Long> content4 = new HashMap<String, Long>();
		// content4.put("tx.log", 1040L);
		//
		// List<String> tagIds = new ArrayList<String>();
		// String tagId1 = cp.getTagId("agent1", "tx.log");
		// String tagId2 = cp.getTagId("agent1", "tx.log");
		// String tagId3 = cp.getTagId("agent1", "tx.log");
		// String tagId4 = cp.getTagId("agent1", "tx.log");
		//
		// cp.addPendingQ(tagId1, "agent1", content1);
		// cp.addPendingQ(tagId2, "agent2",content2);
		// cp.addPendingQ(tagId3, "agent1",content3);
		// cp.addPendingQ(tagId4, "agent2",content4);
		//
		// Thread.sleep(20000);
		// log.info("stopTagCkecker");
		// cp.stopTagChecker("agent1");
	}

}
