package com.nexr.data.sdp.rolling;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class RollingManager {
	private static final Log log = LogFactory.getLog(RollingManager.class);

	public static String POST_SOURCE_PATH = "/post-result";
	public static String POST_MR_INPUT_PATH = "/postInput";
	public static String POST_MR_OUTPUT_PATH = "/postOutput";

	Configuration conf;
	FileSystem fs;

	public RollingManager() {
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static void main(String args[]) {
		if(args[0].trim().equals("hour")){
			HourlyRollingJob job = new HourlyRollingJob();
			if(job.checkFile()>0){
				job.initHourlyMr();
				job.moveHourlyMrData();
				job.runHourlyMr();
				job.moveHourlyMrResult();
				job.endHourlyMr();
			}
		}else{
			DailyRollingJob drjob = new DailyRollingJob();
			if(drjob.checkFile() > 0){
				drjob.initDailyMr();
				drjob.moveDailyMrData();
				drjob.runHourlyMr();
				drjob.moveDailyMrResult();
			}
		}
	}
}
