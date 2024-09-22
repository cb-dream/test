package com.cb.kafka.produce.RDDFuction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LambdaTest {
    private static final Logger log = LoggerFactory.getLogger(LambdaTest.class);



    public static void main(String[] args) {
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

        //        list.forEach(new Consumer<Integer>() {
//
//            @Override
//            public void accept(Integer integer) {
//                System.out.println(integer);
//            }
//        });

        list.forEach(val -> System.out.println(val));
        list.forEach(System.out::println);
        list.sort((v1,v2) -> v1.getAge()+ v2.getAge());
        System.out.println("====");
        list.stream().reduce((a, b) ->new Person(a.getName(),a.getAge() + b.getAge(),a.getSize() + b.getSize())).ifPresent(System.out::println);
        list.stream().distinct().collect(Collectors.toList());
        list.stream().sorted((v1,v2) ->-v1.compareTo(v2)).collect(Collectors.toList());
        list.stream().filter(v1 -> v1.getAge() > 3).collect(Collectors.toList());

//        List<String> collect = list.stream().map(Person::toString).collect(Collectors.toList());
        list.stream().sorted((l1, l2)->- l1.getAge() + l2.getAge()).forEach(System.out::println);
//        list.stream().distinct().forEach(System.out::println);
//        list.stream().map(Person::getName).forEach(System.out::println);
//        list.stream().map(val ->val.getName()).forEach(System.out::println);
//        double sum = list.stream().mapToDouble(Person::getAge).sum();
//        Map<String, Map<String, List<Person>>> collect = list.stream().collect(Collectors.groupingBy(v1 -> v1.getName(), Collectors.groupingBy(t -> t.getName())));
//        collect.forEach((k, v) -> {
//            System.out.println(k +"----"+v);
//        });
//    }
//            list.sort((v1,v2)->{return v1.getAge() -v2.getAge();});
//            list.forEach(System.out::println);
        Integer i = list.stream().map(Person::getAge).reduce(Integer::sum).get();
        System.out.println(i);

    }
}
