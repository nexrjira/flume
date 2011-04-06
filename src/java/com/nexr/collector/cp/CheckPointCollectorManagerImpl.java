package com.nexr.collector.cp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nexr.cp.thrift.CheckPointService;

public class CheckPointCollectorManagerImpl implements
		CheckPointCollectorManager {
	static final Logger log = LoggerFactory
			.getLogger(CheckPointCollectorManagerImpl.class);

	private Object sync = new Object();

	private final List<String> pendingList = new ArrayList<String>();
	private final List<String> completeList = new ArrayList<String>();

	CheckPointService.Processor processor = new CheckPointService.Processor(
			new CheckPointHandler());
	TNonblockingServerSocket socket;
	TNonblockingServer.Args arguments;
	TServer server;

	public static CheckPointCollectorManager manager = null;

	public static CheckPointCollectorManager getInstance() {
		if (manager == null) {
			return new CheckPointCollectorManagerImpl();
		} else
			return manager;
	}

	public CheckPointCollectorManagerImpl() {
		try {
			socket = new TNonblockingServerSocket(0);
			arguments = new TNonblockingServer.Args(socket);
			arguments.processor(processor);
			server = new TNonblockingServer(arguments);

			server.serve();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void addPandingList(String tagId) {
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
		List<String> res = new ArrayList<String>();
		Iterator<String> it = completeList.iterator();
		while (it.hasNext()) {
			String v = it.next();
			if (v.startsWith(agentName)) {
				res.add(v);
				it.remove();
			}
		}
		return res;
	}

	public static void main(String args[]) {
		CheckPointCollectorManager cpam = new CheckPointCollectorManagerImpl();
		cpam.addPandingList("a1_test.lg_00000001.20110406-092954172+0900.2978085774989");
		cpam.addPandingList("a1_test.lg_00000001.20110406-092954172+0900.2978085774990");
		cpam.addPandingList("a2_test.lg_00000001.20110406-092954172+0900.2978085774989");
		cpam.addPandingList("a2_test.lg_00000001.20110406-092954172+0900.2978085774990");
		cpam.addPandingList("a1_test.lg_00000001.20110406-092954172+0900.2978085774991");
		cpam.addPandingList("a2_test.lg_00000001.20110406-092954172+0900.29780857749191");
		cpam.addPandingList("a3_test.lg_00000001.20110406-092954172+0900.2978085774990");
		cpam.addPandingList("a4_test.lg_00000001.20110406-092954172+0900.2978085774989");

		cpam.moveToCompleteList();

		List<String> agentTags = cpam.getTagList("a3_test.lg");
		Iterator<String> it = agentTags.iterator();
		while (it.hasNext()) {
			log.info(it.next());
		}

	}
}
