package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class ScalaVal {


    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("hello");
        objects.add("world");
        objects.add(123);
        objects.add(123);
        objects.forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
