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

import java.util.regex.Pattern;

/**
 * Created by jayantshekhar
 */
public class DatasetHousing {

    public static JavaRDD<LabeledPoint> createRDD(JavaSparkContext sc, String inputFile) {

        JavaRDD<String> data = sc.textFile(inputFile);

        data = data.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.contains("price"))
                    return false;

                return true;
            }
        });

        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");

                        // label : price
                        Double price = Double.parseDouble(parts[1]);

                        double[] v = new double[4];
                        v[0] = Double.parseDouble(parts[2]); // lotsize
                        v[1] = Double.parseDouble(parts[3]); // bedrooms
                        v[2] = Double.parseDouble(parts[4]); // bathrooms
                        v[3] = Double.parseDouble(parts[5]); // stories

                        return new LabeledPoint(price, Vectors.dense(v));
                    }
                }
        );

        return parsedData;
    }

}
