package com.gcs.batchcore.batchlayer.producer;

import java.sql.SQLException;

import com.gcs.batchcore.batchlayer.producer.AnalysticStrategy.*;

public interface IproducerService {
    /**
     * Prepare the necessary data for analysis.
     * @param databse : database to get data 
     * @param output : location to export data
     * @throws SQLException 
     */
    void prepareData(String database, String output) throws SQLException;
    
    /**
     * Run machine learning algorithm on the data.
     * @param strategy : algorithm for analyze data
     * @param input : location of input data 
     * @param output : location of output data
     */
    void analyzeData(IanalysticStrategy strategy, String input, String output);
}
