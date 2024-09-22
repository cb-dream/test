package com.cb.kafka.produce;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

public class RDDTest {
    public static void main(String[] args) {
        System.out.println("============");
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        ClassTag<Iterator> v = null;
        ClassTag<Iterator> k = null;
        RDD rdd = new RDD<Iterator>((RDD<?>) k,v) {
            @Override
            public Iterator compute(Partition split, TaskContext context) {
                return null;
            }

            @Override
            public Partition[] getPartitions() {
                return new Partition[0];
            }
        };
        // 4. 关闭sc
        sc.stop();
    }
}
