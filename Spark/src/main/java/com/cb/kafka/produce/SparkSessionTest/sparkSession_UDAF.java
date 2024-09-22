package com.cb.kafka.produce.SparkSessionTest;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.execution.aggregate.HashMapGenerator;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.Function2;

import java.io.Serializable;

import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.functions.udf;

public class sparkSession_UDAF {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

        Dataset<Row> json = session.read().json("data/user.json");

        json.printSchema();

        json.createOrReplaceTempView("user");

        session.udf().register("avg", udaf(new MyUDAF(), Encoders.LONG()));

        session.sql("select avg(age) from user")
                        .show();

        session.close();

    }

    @Data
    public static class Buffer implements Serializable {
        private Long sum;
        private Long count;

        public Buffer(Long count, Long sum) {
            this.count = count;
            this.sum = sum;
        }

        public Buffer() {
        }
    }

    public static class MyUDAF extends Aggregator<Long,Buffer,Double> {
        @Override
        public Buffer zero() {
            return new Buffer(0L, 0L);
        }

        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setSum(b.getSum() + a);
            b.setCount(b.getCount() + 1);
            return b;
        }

        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }

        @Override
        public Encoder<Buffer> bufferEncoder() {
            return Encoders.kryo(Buffer.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

}
