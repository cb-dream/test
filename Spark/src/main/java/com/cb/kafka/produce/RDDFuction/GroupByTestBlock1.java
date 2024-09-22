package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupByTestBlock1 {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
       List<Integer> list =
                Arrays.asList(1,2,9,22,4,88,7,4);
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
        List<Tuple2<String,Integer>> tuple2List =
                Arrays.asList(
                        new Tuple2("a",1),
                        new Tuple2("b",4),
                        new Tuple2("b",2),
                        new Tuple2("c",3)
                );
//        sc.parallelize(list).groupBy(s -> s % 2 ==1).collect().forEach(System.out::println);
        sc.parallelize(list,3).distinct(2).collect().forEach(System.out::println);
//        sc.parallelize(list,3).sortBy(s ->s ,false,2).collect().forEach(System.out::println);
//        sc.parallelize(list,3).sortBy(v -> v+"" ,false,2).collect().forEach(System.out::println);
//        sc.parallelize(people,3).sortBy(v -> v ,false,2).collect().forEach(System.out::println);
//        System.out.println("=======");
//        (a,1)
//        (b,4)
//        (b,2)
//        (c,3)
        sc.parallelizePairs(tuple2List,3).sortByKey().collect().forEach(System.out::println);
//        sc.parallelizePairs(tuple2List,3).groupByKey().collect().forEach(System.out::println);
//        sc.parallelizePairs(tuple2List,3).groupByKey().map(v -> {return new Tuple2(v._1,v._2.spliterator().estimateSize());}).collect().forEach(System.out::println);
//        sc.parallelizePairs(tuple2List, 3).groupBy(v -> v._1).collect().forEach(System.out::println);
        sc.parallelizePairs(tuple2List, 3).coalesce(2).collect().forEach(System.out::println);
        sc.parallelizePairs(tuple2List, 3).coalesce(2,true).collect().forEach(System.out::println);
        sc.parallelizePairs(tuple2List, 3).repartition(2).collect().forEach(System.out::println);
//        sc.textFile("data/w*").flatMap(s -> Arrays.asList(s.split(" ")).iterator()).groupBy(s -> s).collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
