package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class GroupByTest {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
       List<Integer> list =
                Arrays.asList(1,2,3,4);
//        sc.parallelize(list).groupBy(s -> s % 2 ==1).collect().forEach(System.out::println);
        sc
                .textFile("data/w*")
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .groupBy(s -> s)
                .collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
