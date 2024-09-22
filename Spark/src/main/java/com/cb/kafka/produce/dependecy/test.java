package com.cb.kafka.produce.dependecy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class test {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> rdd = sc
                .<String,Integer>parallelizePairs(Arrays.asList(
                        new Tuple2<String, Integer>("a", 3),
                        new Tuple2<String, Integer>("b", 2),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("a", 1)
                ));
        JavaRDD<Integer> rdd1 = sc
                .parallelize(Arrays.asList(
                        1, 2, 3, 4, 5, 6, 7
                ));
        rdd
                .mapToPair(n -> new Tuple2<>(n._1,n._2 * 10) )

                .mapValues(n -> n * 0.4)
                .sample(true, 2)
                
                .collect()

                .forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }
}
