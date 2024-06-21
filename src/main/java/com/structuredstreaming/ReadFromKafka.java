package com.structuredstreaming;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class ReadFromKafka {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession session = SparkSession.builder()
                .appName("ReadFromKafka")
                .master("local[1]")
                .getOrCreate();

        // Define the schema to match your JSON data
        StructType schema = new StructType()
                .add("product", DataTypes.StringType)
                .add("total_profit", DataTypes.DoubleType)
                .add("valid_items", DataTypes.IntegerType)
                .add("sold", DataTypes.IntegerType);

        // Read data from Kafka and apply schema
        Dataset<Row> readData = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic1")
                .option("startingOffsets", "earliest")  // Read from the beginning of the topic
                .load()
                .selectExpr("CAST(value AS STRING) as json");

        // Define the schema and use from_json to parse JSON data
        Dataset<Row> jsonData = readData.select(from_json(col("json"), schema).as("data"));

        // Use foreachBatch to process each batch of streaming data
        StreamingQuery query = jsonData.writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // Check if the batchDF is not empty
                    if (!batchDF.isEmpty()) {
                        // Extract the inner "data" column and show each row in JSON format
                        batchDF.select("data.*").toJSON().show(false);
                    } else {
                        // Handle the case when there is no data in the batch
                        System.out.println("No data in the batch.");
                    }
                })
                .start();

        query.awaitTermination();
    }
}
