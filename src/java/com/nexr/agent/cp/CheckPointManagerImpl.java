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

	private final Map<String, Map<String, Long>> pendingQ;
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

	public static CheckPointManager manager = null;

	public static CheckPointManager getInstance() {
		if (manager == null) {
			manager = new CheckPointManagerImpl();
			return manager;
		} else
			return manager;
	}

	public CheckPointManagerImpl() {
		agentList = new ArrayList<String>();
		pendingQ = new HashMap<String, Map<String, Long>>();
		checkPointFilePath = FlumeConfiguration.get().getCheckPointFile();
		checkTagIdThread = new CheckTagIDThread();
		pendingList = new ArrayList<String>();
		completeList = new ArrayList<String>();
	}

	@Override
	public void startClient(String collector) {
		// TODO Auto-generated method stub
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

	@Override
	public void startServer() {

		log.info("Server Started");
		try {
			processor = new CheckPointService.Processor(new CheckPointHandler());
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

	public Map<String, Long> getOffset(List<String> tagIds) {
		// TODO Auto-generated method stub
		Map<String, Long> res = new HashMap<String, Long>();
		for (int tagId = 0; tagId < tagIds.size(); tagId++) {
			if (pendingQ.containsKey(tagIds.get(tagId))) {
				Map<String, Long> tagContents = pendingQ.get(tagIds.get(tagId));
				Set<String> keySet = tagContents.keySet();
				Object[] keys = keySet.toArray();
				for (int i = 0; i < keys.length; i++) {
					String key = keys[i].toString();
					if (!res.containsKey(key)) {
						res.put(key, tagContents.get(key));
					} else {
						long lastOffset = res.get(key);
						if (lastOffset <= tagContents.get(key)) {
							res.put(key, tagContents.get(key));
						}
					}

				}
				pendingQ.remove(tagIds.get(tagId));
			}
		}
		return res;
	}

	public void updateCheckPointFile(String logicalNodeName, List<String> tagIds) {
		// TODO Auto-generated method stub
		Map<String, Long> res = getOffset(tagIds);

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

					while ((line = reader.readLine()) != null) {
						compareMap.put(
								line.substring(0, line.indexOf(SEPERATOR))
										.trim(),
								line.substring(line.indexOf(SEPERATOR),
										line.length()).trim());
					}
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

	@Override
	public Map<String, Long> getOffset(String logicalNodeName) {
		// TODO Auto-generated method stub
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
//					startClient(collectorHost);
					checkTagID();
					Clock.sleep(checkTagIdPeriod);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			stopped.countDown();
		}

	};

	@Override
	public void startTagCheck() {
		checkTagIdThread.start();
	}

	public void stop() {
		CountDownLatch stopped = checkTagIdThread.stopped;
		checkTagIdThread.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			log.error("Problem waiting for livenessManager to stop", e);
		}
	}

	public void checkTagID() {
		// TODO Auto-generated method stub
		// tagID null check
		log.info("Check Server TagID " + agentList.size());
		log.info(agentList.get(0).toString());
		List<String> tagIds = null;
		try {
			for (int i = 0; i < agentList.size(); i++) {
				tagIds = client.checkTagId(agentList.get(i));
				log.info("TagSize " + tagIds.size());
				updateCheckPointFile(agentList.get(i), tagIds);

			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void addPendingQ(String tagId, Map<String, Long> tagContent) {
		// TODO Auto-generated method stub
		pendingQ.put(tagId, tagContent);
		Log.info("add " + tagId + " into PendingQ");
	}

	@Override
	public void addCollectorPendingList(String tagId) {
		// TODO Auto-generated method stub
		// compareStr agentName_fileName
		String compareStr = tagId.substring(0, tagId.lastIndexOf("_"));
		boolean isExist = false;

		for (int i = 0; i < pendingList.size(); i++) {
			if (pendingList.get(i).startsWith(compareStr)) {
				isExist = true;
			}
		}

		if (isExist) {
			synchronized (sync) {
				for (int i = 0; i < pendingList.size(); i++) {
					if (pendingList.get(i).startsWith(compareStr)) {
						pendingList.remove(i);
						pendingList.add(tagId);
					}
				}
			}
		} else {
			pendingList.add(tagId);
		}
	}

	@Override
	public void moveToCompleteList() {
		// TODO Auto-generated method stub
		Iterator<String> it = pendingList.iterator();
		synchronized (sync) {
			while (it.hasNext()) {
				completeList.add(it.next());
				it.remove();
			}
		}

		log.info("PendingList " + pendingList.size());

		for (int i = 0; i < completeList.size(); i++) {
			log.info("complete " + completeList.get(i));
		}
	}

	@Override
	public List<String> getTagList(String agentName) {
		// TODO Auto-generated method stub
		// return and delete by agentName
		System.out.println(agentName + " " + completeList.size());
		List<String> res = new ArrayList<String>();
		Iterator<String> it = completeList.iterator();
		while (it.hasNext()) {
			String v = it.next();
			log.info("Complete QUEUE : " + v);
			if (v.startsWith(agentName)) {
				res.add(v);
				it.remove();
			}
		}
		return res;
	}

}
