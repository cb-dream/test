package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class sparkSession_hive {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "cb");

        SparkSession session = SparkSession.builder()

                .enableHiveSupport().config(new SparkConf().setAppName("SparkSessionTest").setMaster("local[*]")).getOrCreate();

        session.sql("use default");
        session.sql("show tables from default").show();
        //session.sql("create table user_info_test(name STRING,age bigint)");
        session.sql("insert into table user_info_test values('csb',18)");
        session.sql("select * from user_info_test").show();
        session.close();

    }
}
