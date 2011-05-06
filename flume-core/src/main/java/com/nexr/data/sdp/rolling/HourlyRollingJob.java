package com.nexr.data.sdp.rolling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;

import com.nexr.data.sdp.rolling.mr.HourlyRollingMr;

public class HourlyRollingJob {
	
	private static final Log log = LogFactory.getLog(HourlyRollingJob.class);
	
	public static String RAW_PATH = "/repo";
	public static String HOURLY_MR_RESULT_PATH = "/hour-result";
	public static String HOURLY_MR_INPUT_PATH = "/hourlyInput";
	public static String HOURLY_MR_OUTPUT_PATH = "/hourlyOutput";

	final public static PathFilter DATA_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().endsWith(".done");
		}
	};
	
	final public static PathFilter SEQ_FILE_FILTER = new PathFilter() {
		public boolean accept(Path file) {
			return file.getName().startsWith("part");
		}
	};
	
	Configuration conf;
	FileSystem fs;
	
	public HourlyRollingJob() {
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public int checkFile() {
		int fileCount = 0;
		Path sourcePath = new Path(RAW_PATH);
		FileStatus[] dataType = null;
		FileStatus[] dataFile = null;
		try {
			dataType = fs.listStatus(sourcePath);
			for (FileStatus type : dataType) {
				dataFile = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()), DATA_FILTER);
				fileCount += dataFile.length;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileCount;
	}
	
	public boolean initHourlyMr() {
		boolean result = true;
		Path hourlyInput = new Path(HOURLY_MR_INPUT_PATH);
		Path hourlyOutput = new Path(HOURLY_MR_OUTPUT_PATH);
		try {
			if (!fs.exists(hourlyInput)) {
				fs.mkdirs(hourlyInput);
			}
			if (fs.exists(hourlyOutput)) {
				fs.delete(hourlyOutput);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			result = false;
			e.printStackTrace();
		}
		log.info("Success Initialization : Create " + HOURLY_MR_INPUT_PATH);
		return result;
	}

	public boolean moveHourlyMrData() {
		boolean result = true;
		Path sourcePath = new Path(RAW_PATH);
		FileStatus[] dataType = null;
		FileStatus[] dataFile = null;
		try {
			dataType = fs.listStatus(sourcePath);
			for (FileStatus type : dataType) {
				dataFile = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()), DATA_FILTER);
				for (FileStatus file : dataFile) {
					if (!fs.exists(new Path(HOURLY_MR_INPUT_PATH
							+ File.separator + type.getPath().getName()))) {
						fs.mkdirs(new Path(HOURLY_MR_INPUT_PATH
								+ File.separator + type.getPath().getName()));
					}
					
					if(dataFile.length>0){
						log.info("Fine File " + file.getPath());
						boolean rename = fs.rename(file.getPath(), new Path(
								HOURLY_MR_INPUT_PATH + File.separator
										+ type.getPath().getName() + File.separator
										+ file.getPath().getName()));
						log.info("Moving " + file.getPath() + " to "
								+ HOURLY_MR_INPUT_PATH + File.separator
								+ type.getPath().getName() + File.separator
								+ file.getPath().getName() + ", status is: "
								+ rename);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public boolean runHourlyMr() {
		List<String> params = new ArrayList<String>();
		params.add(HOURLY_MR_INPUT_PATH + File.separator + "*" + File.separator
				+ "*.done");
		params.add(HOURLY_MR_OUTPUT_PATH);

		try {
			String[] args = params.toArray(new String[params.size()]);
			return ToolRunner.run(conf, new HourlyRollingMr(), args) == 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean moveHourlyMrResult() {
		boolean result = true;
		Path sourcePath = new Path(HOURLY_MR_OUTPUT_PATH);
		FileStatus[] dataType = null;
		FileStatus[] hours = null;
		FileStatus[] files = null;
		try {
			if (!fs.exists(new Path(HOURLY_MR_RESULT_PATH))) {
				fs.mkdirs(new Path(HOURLY_MR_RESULT_PATH));
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			dataType = fs.listStatus(sourcePath);
			boolean rename;
			String dirName;
			for (FileStatus type : dataType) {
				hours = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus hour : hours) {
					files = fs.listStatus(new Path(sourcePath, type.getPath()
							.getName()+File.separator+hour.getPath()
						.getName()), SEQ_FILE_FILTER);
					for (FileStatus file : files) {
						log.info("Find File " + file.getPath());
						
						dirName = hour.getPath().getName();
						
						Path dest = new Path(HOURLY_MR_RESULT_PATH
								+ File.separator + type.getPath().getName()
								+ File.separator + hour.getPath().getName());
								//+ File.separator + file.getPath().getName());
						log.info("Dest " + dest.toString());
						
						if (!fs.exists(dest)) {
							fs.mkdirs(dest);
							rename = fs.rename(file.getPath(), new Path(dest+File.separator+file.getPath().getName()));
						}else{
							//deduplication ?¤í–‰
							rename = fs.rename(file.getPath(), new Path(dest+File.separator+file.getPath().getName()));
						}
					
						log.info("Moving " + file.getPath() + " to "
						+ new Path(dest+File.separator+file.getPath().getName()).toString() + " "
						+ rename);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public boolean endHourlyMr() {
		boolean result = true;
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(HOURLY_MR_INPUT_PATH));
			fs.delete(new Path(HOURLY_MR_OUTPUT_PATH));
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			result = false;
			e.printStackTrace();
		}
		return result;
	}
}
