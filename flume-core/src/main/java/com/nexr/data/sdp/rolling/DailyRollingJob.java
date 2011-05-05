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

import com.nexr.data.sdp.rolling.mr.DailyRollingMr;

public class DailyRollingJob {
	
	private static final Log log = LogFactory.getLog(DailyRollingJob.class);
	
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
	
	public static String HOURLY_MR_RESULT_PATH = "/hour-result";
	public static String DAILY_MR_RESULT_PATH = "/day-result";
	public static String DAILY_MR_INPUT_PATH = "/dailyInput";
	public static String DAILY_MR_OUTPUT_PATH = "/dailyOutput";
	
	Configuration conf;
	FileSystem fs;
	
	public DailyRollingJob() {
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
		Path sourcePath = new Path(HOURLY_MR_RESULT_PATH);
		FileStatus[] dataType = null;
		FileStatus[] hours = null;
		FileStatus[] dataFile = null;
		try {
			dataType = fs.listStatus(sourcePath);
			for (FileStatus type : dataType) {
				hours = fs.listStatus(new Path(sourcePath, type.getPath().getName()));
				for (FileStatus hour : hours) {
					dataFile = fs.listStatus(hour.getPath()) ;
					fileCount += dataFile.length;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileCount;
	}
	
	public boolean initDailyMr() {
		boolean result = true;
		Path input = new Path(DAILY_MR_INPUT_PATH);
		Path output = new Path(DAILY_MR_OUTPUT_PATH);
		try {
			if (!fs.exists(input)) {
				fs.mkdirs(input);
			}
			if (fs.exists(output)) {
				fs.delete(output);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			result = false;
			e.printStackTrace();
		}
		log.info("Success Initialization : Create " + DAILY_MR_INPUT_PATH);
		return result;
	}

	public boolean moveDailyMrData() {
		boolean result = true;
		Path sourcePath = new Path(HOURLY_MR_RESULT_PATH);
		FileStatus[] dataType = null;
		FileStatus[] hours = null;
		FileStatus[] files = null;
		try {
			dataType = fs.listStatus(sourcePath);
			for (FileStatus type : dataType) {
				hours = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus hour : hours) {
					files = fs.listStatus(hour.getPath());
					log.info("===> " + hour.getPath() + " " + files.length);
					for (FileStatus file : files) {
					
						if (!fs.exists(new Path(DAILY_MR_INPUT_PATH
								+ File.separator + type.getPath().getName()))) {
							fs.mkdirs(new Path(DAILY_MR_INPUT_PATH
									+ File.separator + type.getPath().getName()));
						}
						
						log.info("Find File " + file.getPath());
						
						boolean rename = fs.rename(file.getPath() , new Path(
								DAILY_MR_INPUT_PATH + File.separator
											+ type.getPath().getName()));
							log.info("Moving " + file.toString() + ", status is: "
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
		params.add(DAILY_MR_INPUT_PATH + File.separator + "*" + File.separator
				+ "*" + File.separator + "*");
		params.add(DAILY_MR_OUTPUT_PATH);

		try {
			String[] args = params.toArray(new String[params.size()]);
			return ToolRunner.run(conf, new DailyRollingMr(), args) == 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean moveDailyMrResult() {
		boolean result = true;
		Path sourcePath = new Path(DAILY_MR_OUTPUT_PATH);
		FileStatus[] dataType = null;
		FileStatus[] days = null;
		FileStatus[] files = null;
		try {
			if (!fs.exists(new Path(DAILY_MR_RESULT_PATH))) {
				fs.mkdirs(new Path(DAILY_MR_RESULT_PATH));
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
				days = fs.listStatus(new Path(sourcePath, type.getPath()
						.getName()));
				for (FileStatus day : days) {
					files = fs.listStatus(new Path(sourcePath, type.getPath()
							.getName()+File.separator+ day.getPath()
						.getName()), SEQ_FILE_FILTER);
					
					for (FileStatus file : files) {
						log.info("Find File " + file.getPath());
						dirName = day.getPath().getName();
						
						Path dest = new Path(DAILY_MR_RESULT_PATH
								+ File.separator + type.getPath().getName()
								+ File.separator + day.getPath().getName());
						
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

	public boolean endDailyMr() {
		boolean result = true;
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(DAILY_MR_INPUT_PATH));
			fs.delete(new Path(DAILY_MR_OUTPUT_PATH));
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			result = false;
			e.printStackTrace();
		}
		return result;
	}
	
}
