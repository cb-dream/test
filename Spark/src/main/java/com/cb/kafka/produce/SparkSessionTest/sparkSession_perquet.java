package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class sparkSession_perquet {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

        Dataset<Row> csv = session.read()
                .option("header", "true")
                //.option("inferSchema", "true")
                .option("delimiter", ",")
                .option("sep", ",")
                .option("quote", "\"")
                .option("escape", "\"")
                .csv("data/user.csv");

//        csv.as(Encoders.kryo(User.class)).show();
        csv.show();
        Dataset<User> map = csv.map(
                (MapFunction<Row, User>) v -> {
                     System.out.println("Long.valueOf(v.getString(0))" +Long.valueOf(v.getString(0)));
                        return new User(
                                Long.valueOf(v.getString(0)), v.getString(1));
                }, Encoders.bean(User.class));
        map
                .show();

        map
                .write()
                .parquet("output1");

        session.read().parquet("output1").show();
        
        session.close();

    }
}
