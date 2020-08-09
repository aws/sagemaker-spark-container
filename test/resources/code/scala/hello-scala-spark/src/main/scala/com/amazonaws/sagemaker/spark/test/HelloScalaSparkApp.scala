package com.amazonaws.sagemaker.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
// json4s-native is declared as a managed dependency in SBT. Note that json-core is bundled with Spark.
import org.json4s.native

object HelloScalaSparkApp {
  def main(args: Array[String]): Unit = {
    println("Hello World, this is Scala-Spark!")

    println("Parsing command-line args: " + args.mkString(" "))
    val argsMap = parseArgsMap(args)
    println("Parsed command-line args: " + argsMap.mkString("{", ", ", "}"))
    val inputPath = argsMap("input")
    val outputPath = argsMap("output")

    val spark = SparkSession.builder
      .appName("Hello Spark App")
      .getOrCreate()
    import spark.implicits._
    println("Got a Spark session with version: " + spark.version)

    spark.sparkContext.parallelize(Seq("Hello", "Hola", "Bonjour")).foreach { case greeting: String =>
      System.err.println(s"I'm an executor, $greeting!")
    }

    // Load test data set
    println("Reading input from: " + inputPath)
    val salesDF = spark.read.json(inputPath)
    salesDF.printSchema()

    // This line minimally asserts that json4s-native is correctly included as an external runtime dependency jar
    // TODO: Do something more substantial here, e.g. map to a case class, join with original dataframe.
    //       The difficult part is juggling Java date type conversions.
    println("Parsing first line with json4s: " +
      salesDF.toJSON.rdd.map(native.parseJson(_)).first())

    salesDF.createOrReplaceTempView("sales")
    val topDF = spark.sql("SELECT date, sale FROM sales WHERE sale > 750 SORT BY sale DESC")
    // Show the first 20 rows of the dataframe
    topDF.show()

    // Calculate average sales by date
    val avgDF = salesDF.groupBy("date").avg().orderBy("date")
    println(avgDF.collect)

    // Define a UDF that doubles an integer column
    val doubleUdf = udf((x: Int) => x + x)

    // Apply UDF to sales data
    val saleDoubleDF = salesDF.select($"date", $"sale", doubleUdf($"sale").alias("sale_double")).orderBy($"date", $"sale")
    saleDoubleDF.show()

    // Save the resulting DF to file
    println("Writing output to: " + outputPath)
    saleDoubleDF.coalesce(1).write.json(outputPath)

    spark.stop()
  }

  def parseArgsMap(args: Array[String]): Map[String, String] = {
    args.grouped(2)
      .map( (pair) => {
        pair(0).replace("--","") -> pair(1)
      })
      .toMap
  }
}