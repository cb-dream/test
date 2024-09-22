package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class mapTest {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9),2);

//        rdd.filter(s -> s % 2 == 0).foreach(s-> System.out.println(s));
        rdd.map(s ->  s * 2 ).collect().forEach(System.out::println);
        rdd.map(s ->  s * 2 ).saveAsTextFile("out1");
        // 4. 关闭sc
        sc.stop();
    }
}
