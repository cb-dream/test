package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import scala.Function1;

import javax.validation.constraints.Max;

public class sparkSession {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

        Dataset<Row> json = session.read().json("data/user.json");
        json
                .show();

        json.as(Encoders.bean(User.class)).show();
        System.out.println(" map : ----");
        Dataset<User> map = json.map(
                (MapFunction<Row, User>) n -> new User(n.getLong(0), n.getString(1))
                , Encoders.bean(User.class));

        map
                .sort(new Column("age"))
                //.show()
                .groupBy(new Column("name"))
                .count()
                .show();
        System.out.println(" group by : ------");

        map.groupByKey(( MapFunction<User, String>)  v -> v.getName(),Encoders.STRING() )
                .reduceGroups((ReduceFunction<User>) (user, t1) -> new User(Math.max(user.getAge(),t1.getAge()),user.getName())).show();

        session.close();
    }
}
