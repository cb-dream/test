package com.cb.kafka.produce.Bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class sparkSession_hive_test {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "cb");

        SparkSession session = SparkSession.builder()

                .enableHiveSupport()
                .config(
                        new SparkConf().
                                setAppName("SparkSessionTest").
                                setMaster("local[*]")).
                getOrCreate();

         session.sql("select area,\n" +
                 "     product_name\n" +
                 "from (select *,dense_rank() over (partition by area order by l desc) k\n" +
                 "from (select  area,\n" +
                 "     product_name,count(*) l\n" +
                 " from (select\n" +
                 "     area,\n" +
                 "     p.product_name\n" +
                 "     from (select\n" +
                 "     city_id,\n" +
                 "     click_product_id\n" +
                 " from user_visit_action where click_product_id != -1) uva\n" +
                 "         join city_info c on uva.city_id =c.city_id\n" +
                 "         join product_info p on uva.click_product_id = p.product_id) t1\n" +
                 "    group by t1.area, t1.product_name )t2 )t2 where  k<=3")
                 .write()
                 .csv("product_top3");


    }
}
