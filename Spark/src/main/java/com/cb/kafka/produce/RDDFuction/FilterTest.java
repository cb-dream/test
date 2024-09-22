package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.util.Arrays;

public class FilterTest {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        rdd.filter(s -> s % 2 == 0).foreach(s ->System.out.println(s));
        rdd.filter(s -> s % 2 == 0).collect().forEach(System.out::println);

        Tuple3<String, Integer, Integer> tuple3 = new Tuple3<String, Integer, Integer>("zhangsan",30,1001);
        System.out.println(tuple3._1());
        System.out.println(tuple3._2());
        System.out.println(tuple3._3());
        
        // 4. 关闭sc
        sc.stop();
    }
}
