package com.cb.kafka.produce.SparkSessionTest;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class sparkSession_UDF {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

        Dataset<Row> json = session.read().json("data/user.json");

        json.printSchema();

        json.createOrReplaceTempView("user");

        UserDefinedFunction udf = udf(new UDF1<String, String>() {
            @Override
            public String call(String o) throws Exception {
                return o + "|| UDF";
            }
        }, DataTypes.StringType);

        session.udf().register("udf", udf);

        session.sql("select udf(name) new from user")
                .show();
                //.printSchema();

        UserDefinedFunction udf1 = udf(s -> s + " || udf Lambda" , DataTypes.StringType);

        session.udf().register("udf12", udf1);

        session.sql("select udf12(name) new from user")
                 .show();

        session.close();

    }
}
