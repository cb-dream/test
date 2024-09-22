package com.cb.kafka.produce.SparkSessionTest;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Properties;

public class sparkSession_mysql {
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

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "cC123456");

        csv
        .write()
        .mode(SaveMode.Append)
                .jdbc("jdbc:mysql://hadoop102:3306","gmall.testInfo",properties);

        session
                .read()
                .jdbc("jdbc:mysql://hadoop102:3306?gmall","gmall.testInfo",properties)
                .show();
        session.close();

    }
}
