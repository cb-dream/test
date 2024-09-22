package com.cb.kafka.produce.ActionFuction;

import com.cb.kafka.produce.RDDFuction.Person;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class collect {
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
        System.out.println("rdd.count() = "+rdd.count()+"==========");
        Tuple2<String, Integer> first = rdd.first();
        rdd.take(2).forEach(System.out::println);
        System.out.println("===®");
        rdd1.takeOrdered(2).forEach(System.out::println);
        Map<String, Long> stringLongMap = rdd.countByKey();
        System.out.println(stringLongMap);
        Map<Integer, Long> integerLongMap = rdd1.countByValue();
        System.out.println(integerLongMap);
        rdd.foreach(n->System.out.println(n));
//        rdd.saveAsObjectFile("wor");
        rdd1.collect().forEach(System.out::println);

        List<Person> list = new ArrayList<>();
        Person p1 = new Person("张1", 1, 1);
        Person p101 = new Person("张101", 101, 101);
        Person p2 = new Person("张2", 2, 2);
        Person p3 = new Person("张3", 3, 3);
        Person p4 = new Person("张4", 4, 4);
        Person p5 = new Person("张5", 5, 5);
        Person p6 = new Person("张6", 6, 6);
        list.add(p1);
        list.add(p2);
        list.add(p3);
        list.add(p4);
        list.add(p5);
        list.add(p6);
        list.add(p101);
        JavaSerializer javaSerializer = new JavaSerializer();
//        javaSerializer.read(k)

        sc.parallelize(list).foreach(n->System.out.println( "===="));
        sc.parallelize(list).foreachPartition(n -> System.out.println("+++"));
        // 4. 关闭sc
        sc.stop();
    }
}
