package org.example.fileSource

import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Flatten json files in real time.
 */
object JsonFileFlatten extends Serializable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.streaming.stopGracefullyOnShutdown", true)
      .config("spark.sql.streaming.schemaInference", true)  // enable schema inference for reading json files
      .appName("File Source")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawDF = spark.readStream
      .format("json")
      .option("path", "target/input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val flattenedDF: DataFrame = rawDF.select(col("InvoiceNumber"),
      col("CreatedTime"),
      col("StoreID"),
      col("DeliveryAddress.City"),
      col("DeliveryAddress.State"),
      explode(col("InvoiceLineItems")).as("LineItems")
    )
      .withColumn("ItemCode", col("LineItems.ItemCode"))
      .withColumn("ItemDescription", col("LineItems.ItemDescription"))
      .drop("LineItems")

    val query = flattenedDF.writeStream
      .format("json")
      .option("path", "target/output")
      .option("checkpointLocation", "checkpointDir")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .queryName("Flattened Invoice Writer")  // display in Spark UI
      .start()

    query.awaitTermination()
  }
}
