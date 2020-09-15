/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You
 * may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
*/

package com.amazonaws.sagemaker.spark.test;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;

public class HelloJavaSparkApp {
    public static void main(final String[] args) {
        System.out.println("Hello World, this is Java-Spark!");

        final CommandLine parsedArgs = parseArgs(args);
        final String inputPath = parsedArgs.getOptionValue("input");
        final String outputPath = parsedArgs.getOptionValue("output");

        final SparkSession spark = SparkSession.builder()
                .appName("Hello Spark App")
                .getOrCreate();

        System.out.println("Got a Spark session with version: " + spark.version());

        // Load test data set
        System.out.println("Reading input from: " + inputPath);
        final Dataset salesDF = spark.read().json(inputPath);
        salesDF.printSchema();

        salesDF.createOrReplaceTempView("sales");
        final Dataset topDF = spark.sql("SELECT date, sale FROM sales WHERE sale > 750 SORT BY sale DESC");
        // Show the first 20 rows of the dataframe
        topDF.show();

        final Dataset avgDF = salesDF.groupBy("date").avg().orderBy("date");
        System.out.println("Collected average sales: " + StringUtils.join(avgDF.collectAsList()));

        // Define a UDF that doubles an integer column
        spark.sqlContext().udf().register("double", (Long n) -> n + n, DataTypes.LongType);

        final Dataset saleDoubleDF = salesDF
                .selectExpr("date", "sale", "double(sale) as sale_double")
                .orderBy("date", "sale");
        saleDoubleDF.show();

        System.out.println("Writing output to: " + outputPath);
        saleDoubleDF.coalesce(1).write().json(outputPath);

        spark.stop();
    }

    private static CommandLine parseArgs(final String[] args) {
        final Options options = new Options();
        final CommandLineParser parser = new BasicParser();

        final Option input = new Option("i", "input", true, "input path");
        input.setRequired(true);
        options.addOption(input);

        final Option output = new Option("o", "output", true, "output path");
        output.setRequired(true);
        options.addOption(output);

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("HelloScalaSparkApp --input /opt/ml/input/foo --output /opt/ml/output/bar",
                                          options);
            throw new RuntimeException(e);
        }
    }
}
