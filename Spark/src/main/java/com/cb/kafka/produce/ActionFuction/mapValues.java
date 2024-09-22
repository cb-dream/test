package com.cb.kafka.produce.ActionFuction;

import com.cb.kafka.produce.RDDFuction.Person;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class mapValues {

    public static <T1> void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<Person> people = new ArrayList<>();
        Person p1 = new Person("张1", 1, 1);
        Person p101 = new Person("张101", 101, 101);
        Person p2 = new Person("张2", 2, 2);
        Person p3 = new Person("张3", 4, 3);
        Person p4 = new Person("张4", 4, 4);
        Person p5 = new Person("张5", 5, 5);
        Person p6 = new Person("张6", 6, 6);
        people.add(p1);
        people.add(p2);
        people.add(p3);
        people.add(p4);
        people.add(p5);
        people.add(p6);
        people.add(p101);

        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("b", 5),
                new Tuple2<>("c", 3)
        );
        JavaPairRDD<String, Integer> t1T2JavaPairRDD = sc
                .<String, Integer>parallelizePairs(Arrays.asList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("c", 3)
                ));


        t1T2JavaPairRDD
                .mapValues(v->v*10)
                .cache()
                .groupByKey(new MyPartitioner())
                .saveAsTextFile("output");


        // 4. 关闭sc
        sc.stop();
    }
}
 class MyPartitioner extends Partitioner {

    @Override
    public int numPartitions() {
        return 3;
    }

    @Override
    public int getPartition(Object key) {

        return key.equals("a") ? 0 : 1;
    }
}
