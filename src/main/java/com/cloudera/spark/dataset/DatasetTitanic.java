/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * Created by jayantshekhar
 */
public class DatasetTitanic {

    public static DataFrame createDF(SQLContext sqlContext, String inputFile) {
        // options
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("header", "true");
        options.put("path", inputFile);
        options.put("delimiter", ",");

        // create dataframe from input file
        DataFrame df = sqlContext.load("com.databricks.spark.csv", options);

        return df;
    }

    // create an RDD of Vectors from a DataFrame
    public static JavaRDD<LabeledPoint> createLabeledPointsRDD(JavaSparkContext ctx, SQLContext sqlContext, String inputFile) {

        DataFrame df = createDF(sqlContext, inputFile);
        df.printSchema();

        // convert dataframe to an RDD of Vectors
        JavaRDD<LabeledPoint> rdd = df.toJavaRDD().map(new Function<Row, LabeledPoint>() {

            @Override
            public LabeledPoint call(Row row) {

                int survived = toInt(row.getString(1));

                double[] arr = new double[5];

                String sex = row.getString(4); // sex
                if (sex.equals("male"))
                    arr[0] = 0.0;
                else
                    arr[0] = 1.0;
                arr[1] = toDouble(row.getString(5)); // age
                arr[2] = toDouble(row.getString(6)); // SibSp
                arr[3] = toDouble(row.getString(7)); // Parch
                arr[4] = toDouble(row.getString(9)); // Fare

                Vector vector = Vectors.dense(arr);

                LabeledPoint labeledPoint = new LabeledPoint(survived, vector);

                return labeledPoint;
            }
        });

        return rdd;
    }

    public static int toInt(String str) {
        return Integer.parseInt(str);
    }


    public static double toDouble(String str) {
        if (str.length() == 0)
            return 0.0;

        return Double.parseDouble(str);
    }
}
