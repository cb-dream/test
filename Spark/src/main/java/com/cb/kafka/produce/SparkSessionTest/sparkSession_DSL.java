package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

public class sparkSession_DSL {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

        Dataset<Row> json = session.read().json("data/user.json");

        json.createOrReplaceTempView("user");

        session.sql("select * from user").show();

        json
                .select(col("age"),col("name"),concat_ws("|||",col("name") ))
                .show();

        session.close();

    }
}
