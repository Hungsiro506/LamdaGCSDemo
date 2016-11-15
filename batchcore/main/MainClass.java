package com.gcs.batchcore.main;
import java.sql.SQLException;

import com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.AnalysticAlgorithm.DecisionTreeRegression;
import com.gcs.batchcore.batchlayer.producer.ProjectSuccessRatePrediction.ProjectSuccessRatePrediction;
import com.gcs.batchcore.batchlayer.producer.IproducerService;
import com.gcs.batchcore.utils.HBaseConfiguration;
import com.gcs.batchcore.utils.HiveConfiguration;
import com.gcs.batchcore.utils.SparkConfiguration;

public class MainClass {
	public static void main(String[] args) throws SQLException{
		
		 String database = HiveConfiguration.DATABASE_NAME2;
		 String prepareDataOutput = "null now"; 
		 //HDFS location. // server doesn't support this feature now
		 
		 String analyzeDataInput = SparkConfiguration.DATA_PATH_INPUT;
		 //String analyzeDataInput = "/home/gcsadmin/Desktop/input"; <---- local mode
		 
		 // HBase location.
		 String analyzeDataOutput = HBaseConfiguration.TABLENAME ;
		 IproducerService psrp = new ProjectSuccessRatePrediction();
		 psrp.prepareData(database, prepareDataOutput);
		 psrp.analyzeData(new DecisionTreeRegression(), analyzeDataInput, analyzeDataOutput);
		 
		 System.out.println("Success . It's over !");
		
	}
}
