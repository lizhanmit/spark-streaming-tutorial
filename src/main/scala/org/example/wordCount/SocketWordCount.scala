package org.example.wordCount

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, split}

/**
 * Word count with socket connection as data source.
 */
object SocketWordCount extends Serializable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .appName("Socket Word Count")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val lineDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    val wordCountDF: DataFrame = lineDF.withColumn("values", split(col("value"), " "))
      .select(explode(col("values")).as("word"))
      .groupBy("word").count()


    val wordCountQuery = wordCountDF.writeStream
      .format("console")
      .option("checkpointLocation", "checkpointDir")
      .outputMode("complete")
      .start()

    wordCountQuery.awaitTermination()
  }
}
