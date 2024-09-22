package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class sparkSession_SQL {
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
