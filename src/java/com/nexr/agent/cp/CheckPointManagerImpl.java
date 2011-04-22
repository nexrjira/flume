package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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

	private Map<String, List<PendingQueueModel>> agentTagMap; // agent,
																// list<PendingQueueModel>
	private Map<String, WaitingQueueModel> waitedTagList; // agent,
															// WatingQueueModel

	private Object sync = new Object();

	CheckTagIDThread checkTagIdThread;

	String collectorHost;

	List<String> agentList;

	TSocket socket = null;
	TTransport transport = null;
	TProtocol protocol = null;
	CheckPointService.Client client;
	int timeout = 10 * 1000;

	// for collector
	private final List<String> pendingList;
	private final List<String> completeList;

	CheckPointService.Processor processor;
	TNonblockingServerSocket serverSocket;
	TNonblockingServer.Args arguments;
	TServer server;

	public CheckPointManagerImpl() {
		agentList = new ArrayList<String>();
		agentTagMap = new HashMap<String, List<PendingQueueModel>>();
		waitedTagList = new HashMap<String, WaitingQueueModel>();
		checkPointFilePath = FlumeConfiguration.get().getCheckPointFile();
		checkTagIdThread = new CheckTagIDThread();
		pendingList = new ArrayList<String>();
		completeList = new ArrayList<String>();
	}

	
	public void startClient(String collector) {
		// TODO Auto-generated method stub
		// CheckPoint Thrift client
		if (transport == null || !transport.isOpen()) {
			log.info("New Client");
			this.collectorHost = collector;
			socket = new TSocket(collectorHost, FlumeConfiguration.get()
					.getCheckPointPort());
			socket.setTimeout(timeout);
			transport = new TFramedTransport(socket);
			protocol = new TBinaryProtocol(transport);
			client = new CheckPointService.Client(protocol);
			try {
				transport.open();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
	}

	@Override
	public void stopClient() {
		// TODO Auto-generated method stub
		// CheckPoint Thrift client
		transport.close();
	}

	@Override
	public void startServer() {
		if (server == null || !server.isServing()) {
			try {
				processor = new CheckPointService.Processor(
						new CheckPointHandler());
				serverSocket = new TNonblockingServerSocket(FlumeConfiguration
						.get().getCheckPointPort());
				arguments = new TNonblockingServer.Args(serverSocket);
				arguments.processor(processor);
				server = new TNonblockingServer(arguments);
				server.serve();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		log.info("Starting Server");
	}
	
	@Override
	public void stopServer() {
		server.stop();
	}

	@Override
	public void setCollectorHost(String host) {
		this.collectorHost = host;
	}

	@Override
	public String getTagId(String agentName, String fileName) {
		// TODO Auto-generated method stub
		if (!agentList.contains(agentName)) {
			agentList.add(agentName);
		}
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
		// checkpoint 파일에서 해당 logical Node에 해당하는 파일과 offset을 전달.
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
					startClient(collectorHost);
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
	public void startTagChecker() {
		checkTagIdThread.start();
	}

	@Override
	public void stopTagChecker() {
		CountDownLatch stopped = checkTagIdThread.stopped;
		checkTagIdThread.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			log.error("Problem waiting for livenessManager to stop", e);
		}
	}

	@Override
	public void addPendingQ(String tagId, String agentName,
			Map<String, Long> tagContent) {
		// TODO Auto-generated method stub
		List<PendingQueueModel> tags;

		if (!agentList.contains(agentName)) {
			agentList.add(agentName);
		}
		if (agentTagMap.containsKey(agentName)) {
			tags = agentTagMap.get(agentName);
			PendingQueueModel pqm = new PendingQueueModel(tagId, tagContent);
			tags.add(pqm);
			agentTagMap.put(agentName, tags);
		} else {
			tags = new ArrayList<PendingQueueModel>();
			PendingQueueModel pqm = new PendingQueueModel(tagId, tagContent);
			tags.add(pqm);
			agentTagMap.put(agentName, tags);
		}
		Log.info("add " + tagId + " into PendingQ");
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
				log.info("Tag " + tagIds.get(i) + " added");
			}
		}
		log.info("CompleteList Size " + completeList.size());
	}

	@Override
	// Collector에서 바로 CompleteList로 TagId를 넣으면 필요 없음.
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
	// Collecter tagId가 있는지 확인하고 있으면 True를 전달하고 completeList에서 삭제함.
	public boolean getTagList(String tagId) {
		// TODO Auto-generated method stub
		boolean res = false;
		String v = null;
		log.info("CompleteList " + completeList.size());
		for(int i=0; i<completeList.size(); i++){
			if(completeList.get(i).equals(tagId)){
				v = completeList.get(i);
				res = true;
				completeList.remove(i);
			}
		}
		log.info("CompleteTag " + v + " Result " + tagId + " " + res);
		return res;
	}

	public void checkCollectorTagID() {
		// TODO Auto-generated method stub
		// pendingQueue에 있는 agent의 tagId를 모두 체크 해보고
		// 마지막 true리턴 받은 값을 기억했다가 checkpoint파일에 update한다.
		boolean res = true;
		PendingQueueModel currentTagId = null;
		
		try {
			for (int i = 0; i < agentList.size(); i++) {
				List<PendingQueueModel> tags = agentTagMap
						.get(agentList.get(i));
				if(tags != null){
					for (int t = 0; t < tags.size(); t++) {
						res = client.checkTagId(tags.get(t).getTagId());
						currentTagId = tags.get(t);
						log.info("Current TagID " +  currentTagId.getTagId());
						if (res) {
							// 현재 TagId 저장 후 리스트에서 삭제.
							tags.remove(t);
							updateCheckPointFile(agentList.get(i), currentTagId);
						} else {
							if (currentTagId != null) {
								updateWaitingTagList(agentList.get(i), tags.get(t)
										.getTagId(), tags.get(t).getContents());
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
			if (waitedTagList.get(keys[i]).getWaitedTime() == FlumeConfiguration
					.get().getCheckPointTimeout()) {
				log.info("---------------------------------------");
				PendingQueueModel pqm = new PendingQueueModel(waitedTagList
						.get(keys[i]).getTagId(), waitedTagList.get(keys[i])
						.getContents());
				updateCheckPointFile(keys[i].toString(), pqm);
				
				//waitedTagList에서 삭제
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
		BufferedWriter bw;
		StringBuilder contents;

		Map<String, String> compareMap = new HashMap<String, String>();
		synchronized (sync) {
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
								line.substring(0, line.indexOf(SEPERATOR))
										.trim(),
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
					bw.close();

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
					bw.close();
					reader.close();
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
