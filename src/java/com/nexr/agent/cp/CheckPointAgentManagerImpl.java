package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;

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

	public CheckPointAgentManagerImpl() {
		pendingQ = new HashMap<String, Queue<PendingEvent>>();
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

	@Override
	public void updateCheckPointFile(String fileName, String tagId) {
		// TODO Auto-generated method stub
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

	public static void main(String args[]) {
		CheckPointAgentManager cpam = new CheckPointAgentManagerImpl();
		System.out.println(cpam.getTagId("a1", "test.lg"));
		// System.out.println(cpam.getOffset("teaast8.log"));
		// cpam.addPandingQ("teaast3.log", "11112", 20);
		// cpam.addPandingQ("teaast2.log", "11113", 90);
		// cpam.updateCheckPointFile("teaast2.log", "11113");

	}

}
