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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;
import com.nexr.cp.thrift.CheckPointService;

public class CheckPointAgentManagerImpl implements CheckPointAgentManager {
	static final Logger LOG = LoggerFactory
			.getLogger(CheckPointAgentManagerImpl.class);

	private final static String DATE_FORMAT = "yyyyMMdd-HHmmssSSSZ";
	private final String SEPERATOR = "\t";
	private final String LINE_SEPERATOR = "\n";

	private String checkPointFile = FlumeConfiguration.get()
			.getCheckPointFile();
	private File ckpointFile = new File(checkPointFile);

	private final Map<String, Queue<PendingEvent>> pendingQ;
	private Object sync = new Object();

	CheckTagIDThread checkTagIdThread;

	String collectorHost;

	List<String> agentList;
	

	TSocket socket;
	TTransport transport;
	TProtocol protocol;
	CheckPointService.Client client;
	int timeout = 10 * 1000;

	public CheckPointAgentManagerImpl(String collectorHost) {
		this.collectorHost = collectorHost;
		agentList = new ArrayList<String>();
		pendingQ = new HashMap<String, Queue<PendingEvent>>();

		socket = new TSocket(collectorHost, 0);
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
	public void setCollectorHost(String host) {
		this.collectorHost = host;
	}

	@Override
	public String getTagId(String agentName, String fileName) {
		// TODO Auto-generated method stub
		if(!agentList.contains(agentName)){
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

		// a1_test.lg_00000001.20110406-092852074+0900.2916311334892
		String tagId = String.format("%s_%08d.%s.%012d", prefix, pid, f, nanos);

		return tagId;
	}

	@Override
	public void addPandingQ(String filename, String tagId, long offset) {
		// TODO Auto-generated method stub
		PendingEvent pe = new PendingEvent(tagId, offset);

		Queue<PendingEvent> pendList = pendingQ.get(filename);
		if (pendList == null) {
			pendList = new LinkedList<PendingEvent>();
			pendList.offer(pe);
			pendingQ.put(filename, pendList);
		} else {
			pendList.offer(pe);
		}

	}

	public PendingEvent getOffset(String fileName, String tagId) {
		// TODO Auto-generated method stub
		Queue<PendingEvent> pendList = pendingQ.get(fileName);
		PendingEvent pe = null;
		while (true) {
			pe = pendList.peek();
			if (pe.getTagId() == tagId) {
				pendList.remove();
				break;
			} else {
				pendList.remove();
			}
		}
		return pe;
	}

	public void updateCheckPointFile(String tagId) {
		// TODO Auto-generated method stub
		String fileName = tagId.substring(tagId.indexOf("_") + 1,
				tagId.lastIndexOf("_"));
		PendingEvent pe = getOffset(fileName, tagId);

		FileReader fileReader;
		BufferedReader reader;
		FileWriter fw;
		BufferedWriter bw;
		StringBuilder contents;

		synchronized (sync) {
			try {
				if (!ckpointFile.exists()) {
					ckpointFile.createNewFile();
				}

				boolean isWrited = false;
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);
				String line = null;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith(fileName)) {
						isWrited = true;
					}
				}

				if (isWrited) {
					contents = new StringBuilder();
					fileReader = new FileReader(ckpointFile);
					reader = new BufferedReader(fileReader);

					while ((line = reader.readLine()) != null) {
						if (line.startsWith(fileName)) {
							contents.append(fileName + SEPERATOR
									+ pe.getTagId() + SEPERATOR
									+ pe.getOffset() + LINE_SEPERATOR);
						} else {
							contents.append(line + LINE_SEPERATOR);
						}
						fw = new FileWriter(ckpointFile);
						bw = new BufferedWriter(fw);
						bw.write(contents.toString());
						bw.close();
					}
				} else {
					contents = new StringBuilder();
					fileReader = new FileReader(ckpointFile);
					reader = new BufferedReader(fileReader);

					while ((line = reader.readLine()) != null) {
						contents.append(line + LINE_SEPERATOR);
					}
					contents.append(fileName + SEPERATOR + pe.getTagId()
							+ SEPERATOR + pe.getOffset() + LINE_SEPERATOR);
					fw = new FileWriter(ckpointFile);
					bw = new BufferedWriter(fw);
					bw.write(contents.toString());
					bw.close();

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public String getOffset(String fileName) {
		// TODO Auto-generated method stub
		FileReader fileReader;
		BufferedReader reader;
		String result = null;

		try {
			if (!ckpointFile.exists()) {
				return null;
			} else {
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);
				String line = null;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith(fileName)) {
						result = line.substring(line.lastIndexOf(SEPERATOR),
								line.length());
					}
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
					checkTagID();
					Clock.sleep(checkTagIdPeriod);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			stopped.countDown();
		}

	};

	/**
	 * Starts the CheckPoint Tag thread and then returns.
	 */
	public void start() {
		checkTagIdThread.start();
	}

	public void stop() {
		CountDownLatch stopped = checkTagIdThread.stopped;
		checkTagIdThread.done = true;
		try {
			stopped.await();
		} catch (InterruptedException e) {
			LOG.error("Problem waiting for livenessManager to stop", e);
		}
	}

	public void checkTagID() {
		// TODO Auto-generated method stub
		//tagID null check
		List<String> tagIds = null;
		try {
			for(int i=0; i<agentList.size(); i++){
				tagIds = client.checkTagId(agentList.get(i));
				Iterator<String> it = tagIds.iterator();
				while (it.hasNext()) {
					updateCheckPointFile(it.next());
				}
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	public static void main(String args[]) {
		CheckPointAgentManager cpam = new CheckPointAgentManagerImpl(
				"localhost");
		System.out.println(cpam.getTagId("a1", "test.lg"));
		// System.out.println(cpam.getOffset("teaast8.log"));
		// cpam.addPandingQ("teaast3.log", "11112", 20);
		// cpam.addPandingQ("teaast2.log", "11113", 90);
		// cpam.updateCheckPointFile("teaast2.log", "11113");
	}

}
