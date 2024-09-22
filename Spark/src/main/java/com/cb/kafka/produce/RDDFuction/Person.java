package com.cb.kafka.produce.RDDFuction;


import scala.Serializable;

public class Person  implements Serializable, Comparable<Person> {
    private String name;
    private int age;
    private int size;

    public Person() {
        super();
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", size=" + size +
                '}';
    }

    public Person(String name, int age, int size) {
        this.name = name;
        this.age = age;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }



    @Override
    public int compareTo(Person o) {
        if (this.age != o.age)
        return o.getAge()- this.getAge();
        if (this.age == o.age) return this.getName().hashCode()-o.getName().hashCode() ;
        return 0;
    }
}
