package com.nexr.agent.cp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

public class CheckPointAgentManagerImpl implements CheckPointAgentManager {

	private final Map<String, Queue<PendingEvent>> pendingQ;
	private Object sync = new Object();

	public CheckPointAgentManagerImpl() {
		pendingQ = new HashMap<String, Queue<PendingEvent>>();
	}

	@Override
	public String getTagId(String fileName) {
		// TODO Auto-generated method stub
		String tagId = UUID.randomUUID().toString() + fileName.hashCode();
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

		File ckpointFile = new File("D:/data.txt");

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
							contents.append(fileName + " " + pe.getTagId()
									+ " " + pe.getOffset() + "\n");
						} else {
							contents.append(line + "\n");
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
						contents.append(line + "\n");
					}
					contents.append(fileName + " " + pe.getTagId() + " "
							+ pe.getOffset() + "\n");
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
		File ckpointFile = new File("D:/data.txt");
		try {
			if (!ckpointFile.exists()) {
				return null;
			}else{
				fileReader = new FileReader(ckpointFile);
				reader = new BufferedReader(fileReader);
				String line = null;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith(fileName)) {
						result = line.substring(line.lastIndexOf(" " ), line.length());
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String args[]) {
		CheckPointAgentManager cpam = new CheckPointAgentManagerImpl();

		System.out.println(cpam.getOffset("teaast8.log"));
//		cpam.addPandingQ("teaast3.log", "11112", 20);
//		cpam.addPandingQ("teaast2.log", "11113", 90);
//		cpam.updateCheckPointFile("teaast2.log", "11113");

	}

	

}
