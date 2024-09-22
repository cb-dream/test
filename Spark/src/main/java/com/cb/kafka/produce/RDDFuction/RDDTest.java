package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码

        // 4. 关闭sc
        sc.close();
    }
}
