package com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.AnalysticAlgorithm;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import scala.Tuple2;

import com.gcs.batchcore.batchlayer.hbase.DataHBase;
import com.gcs.batchcore.batchlayer.hbase.HBaseService;
import com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.IanalysticStrategy;
import com.gcs.batchcore.infrastructure.Constants;

/**
 *@author hungdv
 *@since 7-8-2015 
 */

public class DecisionTreeRegression implements IanalysticStrategy, Serializable{
	static Logger log = Logger.getLogger(
			DecisionTreeRegression.class.getName());
	private static final long serialVersionUID = 1L;
	@Override
	public void analyzeData(String input, String output) {
		
		
		final Pattern COMMA = Pattern.compile(",");
		SparkConf sparkConf = new SparkConf().setAppName("DecisionTree").setMaster("yarn-cluster");
		log.info("Set app name : DecisionTree");
		log.info("Set Master   : yarn-cluster");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		String datapath = input;
		log.info("data input path : " + input);
		
		// Load and parse the data file.
		JavaRDD<String> text = sc.textFile(datapath); 
		JavaPairRDD<String, LabeledPoint> data = text.mapToPair(new PairFunction<String,String,LabeledPoint>()
				{
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, LabeledPoint> call(String t)
							throws Exception {
						// TODO Auto-generated method stub
						String project = t.substring(0,5);
						String value   = t.substring(6); // careful error occur when sub and to double.
						String[] strs = COMMA.split(value);
						double[] doubleValues = new double[strs.length - 1];
                        for (int i = 0; i < strs.length - 1; i++) {
                            doubleValues[i] = Double.parseDouble(strs[i]);
                        }
                        LabeledPoint labeledPoint = new LabeledPoint(
                        		Double.parseDouble(strs[strs.length - 1]), // header
                        		Vectors.dense(doubleValues));			  // tailer
                        return new Tuple2<String, LabeledPoint>(project, labeledPoint);
					}			
				});
		//  Split the data into training and test sets (30% held out for testing)	
		JavaRDD<LabeledPoint> trainingData = data.values().sample(false,0.7, 12L);
		JavaRDD<LabeledPoint> testData = data.values().subtract(trainingData);
		
		//  Set parameters.
		//  Empty categoricalFeaturesInfo indicates all features are continuous.
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		String impurity = "variance"; //Criterion used for information gain calculation. Supported values: "variance".
		Integer maxDepth = 4; // Maximum depth of the tree. (suggested value: 4)
		Integer maxBins = 100; // Maximum number of bins used for splitting features (suggested value: 100) 

		//  Train a DecisionTree model.
		final DecisionTreeModel model = DecisionTree.trainRegressor(trainingData,
		    categoricalFeaturesInfo, impurity, maxDepth, maxBins);

		//  Evaluate model on test instances and compute test error
		JavaPairRDD<Double, Double> predictionAndLabel =
		    testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Tuple2<Double, Double> call(LabeledPoint p) {
		      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
				}
		    });
		
		//
		Double testMSE = predictionAndLabel.map(new Function<Tuple2<Double,Double> , Double>() {
			private static final long serialVersionUID = 1L;

			@Override
		    public Double call(Tuple2<Double, Double> pl) {
		      Double diff = pl._1() - pl._2();
		      return diff * diff;
		    	}
			}).reduce(new Function2<Double, Double, Double>() {
		   
			private static final long serialVersionUID = 1L;

			public Double call(Double a, Double b) {
		      return a + b;
		    }
		  }) / data.count();
		log.info("Test Mean Squared Error: " + testMSE);
		log.info("Learned regression tree model:\n" + model.toDebugString());
		
		 // Save and load model \\\
		 //////////////////////////
		 // model.save(sc.sc(), output);
		 //	DecisionTreeModel sameModel = DecisionTreeModel.load(sc.sc(), output);
		
		 // Export out put -> csv \\\
		 ///////////////////////////
		 /*JavaRDD<String> outputData = data
	                .map(new Function<Tuple2<String, LabeledPoint>, String>() {
	                    *//**
						 * 
						 *//*
						private static final long serialVersionUID = 1L;
						@Override
	                    public String call(Tuple2<String, LabeledPoint> t) {
	                        String project = t._1;
	                        String attributes = t._2.features().toString()
	                                .replace("[", "").replace("]", "");
	                        double realValue = t._2.label();
	                        double predictValue = model.predict(t._2.features());
	                        double MSE = Math.pow(realValue - predictValue, 2);
	                        String row = project + "," + attributes + ","
	                                + Double.toString(realValue) + ","
	                                + Double.toString(predictValue) + ","
	                                + Double.toString(MSE);
	                        return row;
	                    }
	                });*/
		 
		 	// Save to csv file
		 	//File theDir = new File(output);
		 	//if(!theDir.exists()){
		 	//	theDir.delete();
		 	//}
	        //outputData.saveAsTextFile(output);
	        //log.info("Export output as Text file locate in: "+ output);
	        
	        // Save to HBase
	        try {
            saveToHBase(output,sc,data,model);
	        } catch (IOException e) {
	          
            e.printStackTrace();
            log.info("Error : "+e.getMessage());
            
            }
	        sc.close();
	        
	        log.info("Export data to HBase successfully");
	    }
	 
	      // Save to Hbase ///
	      ////////////////////
	
	      private void saveToHBase(final String outputTableName, JavaSparkContext sc,
	      JavaPairRDD<String, LabeledPoint> data, final DecisionTreeModel model)
	      throws IOException {

	    // Save data to HBase
	    final Pattern COMMA = Pattern.compile(",");
	    
	    final HBaseService hbase = new HBaseService("hbase:" + Constants.HBASE_MASTER_PORT);
	    hbase.openConnection();
	    String[] colF = {"content"};
	    hbase.createTable(outputTableName, colF);
	    hbase.closeConnection();

	    final Broadcast<String[]> broadcastVar =
	        sc.broadcast(new String[] {"projectsize", "bugs", "bugeffort",
	            "oteffort", "totaleffort", "teamsize", "currentsuccessrate", "predict"});

	    data.foreach(new VoidFunction<Tuple2<String, LabeledPoint>>() {
	  
        private static final long serialVersionUID = 1L;

        @Override
	      public void call(Tuple2<String, LabeledPoint> t2) throws IOException {

	        String[] valueNames = broadcastVar.value();
	        String projectid = t2._1;
	        String attributes = t2._2.features().toString().replace("[", "").replace("]", "");
	        double realValue = t2._2.label();
	        double predictValue = model.predict(t2._2.features());

	        String values =
	            attributes + "," + Double.toString(realValue) + "," + Double.toString(predictValue);
	        String[] arrValues = COMMA.split(values);

	        HBaseService hbase = new HBaseService("hbase:" + Constants.HBASE_MASTER_PORT);
	        hbase.openConnection();

	        for (int i = 0; i < arrValues.length; i++) {

	          List<byte[]> colName = new ArrayList<byte[]>();
	          colName.add(Bytes.toBytes(valueNames[i]));
	          List<byte[]> colData = new ArrayList<byte[]>();
	          colData.add(Bytes.toBytes(arrValues[i]));

	          DataHBase data = new DataHBase(Bytes.toBytes(projectid), colName, colData);
	          hbase.addData(outputTableName, "content", new DataHBase[] {data});
	        }
	        hbase.closeConnection();

	      }
	    });
	 }
}
