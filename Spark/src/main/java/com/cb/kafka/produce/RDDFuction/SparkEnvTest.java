package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkEnvTest {
    public static void main(String[] args) {
//        org.apache.spark.SparkConf@5a7fe64f
//        public int defaultParallelism() {
//            return this.scheduler.conf().getInt("spark.default.parallelism", this.totalCores());
//        }
        SparkConf sparkEnv = new SparkConf().setMaster("local[*]").setAppName("SparkEnvTest");
        System.out.println(sparkEnv);
//        org.apache.spark.api.java.JavaSparkContext@5fc930f0
//        set master &  set application name
        JavaSparkContext context = new JavaSparkContext(sparkEnv);

        System.out.println(context);
//        var4 = positions$1((long)var6.length(), numSlices).zipWithIndex().map((x0$1) -> {
//            if (x0$1 != null) {
//                Tuple2 var5 = (Tuple2)x0$1._1();
//                int index = x0$1._2$mcI$sp();
//                if (var5 != null) {
//                    int start = var5._1$mcI$sp();
//                    int end = var5._2$mcI$sp();
//                    Range.Inclusive var3 = var6.isInclusive() && index ==
//                    numSlices - 1 ? new Range.Inclusive(var6.start() + start * var6.step(), var6.end(), var6.step())
//                    : new Range.Inclusive(var6.start() + start * var6.step(), var6.start() + (end - 1) * var6.step(), var6.step());
//                    return var3;
//                }
//            }
        JavaRDD<String> rdd = context.parallelize(Arrays.asList("zhangsan", "lisi", "wangwu","zhaoliu","sunqi"),2);
        JavaRDD<String> stringJavaRDD = context.textFile("data/t*",3);
//        stringJavaRDD.filter(s -> !s .contains( "zhangsan")).foreach(s -> System.out.println(s));
        stringJavaRDD.saveAsTextFile("output");
        //        rdd.collect().stream().reduce();



        context.stop();
    }
}
