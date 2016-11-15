package com.gcs.batchcore.batchlayer.producer.ProjectSuccessRatePrediction;
import java.sql.SQLException;

import com.gcs.batchcore.batchlayer.hive.HivePreparingData;
import com.gcs.batchcore.batchlayer.producer.IproducerService;
import com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.IanalysticStrategy;
import com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.AnalysticAlgorithm.DecisionTreeRegression;
import org.apache.log4j.Logger;
public class ProjectSuccessRatePrediction implements IproducerService {
	
    //static Logger log = Logger.getLogger(
    //    ProjectSuccessRatePrediction.class.getName());
    
	IanalysticStrategy strategy;
	
	@Override
	public  void prepareData(String database,String output) throws SQLException {
	  if(database != null && output != null){
		HivePreparingData hpd = new HivePreparingData(database,output);
		//log.info("data base : " + database);
        //log.info("output : " + output);
	 
	    //log.info("ERROR: database or output is null");
	  
		// import data from data source and selecting necessary data.
		HivePreparingData.init(); 
		//log.info("init data");
		// clean data to clear.
		HivePreparingData.cleanData(); 
		//log.info("clean data");
		//save output to hdfs, then it will be input for analyze.
		//hpd.saveToHDFS(); 
	  }
	}

	@Override
	public  void analyzeData(IanalysticStrategy strategy, String input, String output) {
	  if(input !=  null && output != null){
		strategy.analyzeData(input, output);
		//log.info("Analyze Data");
	  }
	  //log.info("ERROR : input or output is null");
	  
	}
}
