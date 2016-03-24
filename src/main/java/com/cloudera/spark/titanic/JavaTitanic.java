package com.cloudera.spark.titanic;

import com.cloudera.spark.dataset.DatasetMovieLens;
import com.cloudera.spark.dataset.DatasetTitanic;
import com.cloudera.spark.mllib.SparkConfUtil;
import com.cloudera.spark.randomforest.JavaRandomForest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class JavaTitanic {

    // Usage: JavaTitanic <input_file>
    public static void main(String[] args) {

        String inputFile = "data/titanic/train.csv";
        if (args.length >= 2) {
            inputFile = args[0];
        }

        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("JavaTitanic");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // create data frame
        //DataFrame results = DatasetTitanic.createDF(sqlContext, inputFile);

        //results.printSchema();

        // LabeledPoint RDD
        JavaRDD<LabeledPoint> data = DatasetTitanic.createLabeledPointsRDD(sc, sqlContext, inputFile);
data.count();
        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // classification using RandomForest
        System.out.println("\nRunning classification using RandomForest\n");
        JavaRandomForest.testClassification(trainingData, testData);

        // regression using Random Forest
        System.out.println("\nRunning regression using RandomForest\n");
        JavaRandomForest.testRegression(trainingData, testData);

        sc.stop();

    }

}
