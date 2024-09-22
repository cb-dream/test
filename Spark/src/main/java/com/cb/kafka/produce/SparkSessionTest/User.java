package com.cb.kafka.produce.SparkSessionTest;

import lombok.Data;

@Data
public class User {
    private Long age;
    private String name;

    public User(Long age, String name) {
        this.age = age;
        this.name = name;
    }

    public User() {
    }
}
