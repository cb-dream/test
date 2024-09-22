package com.cb.kafka.produce.Bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.*;

import java.lang.Long;
import java.util.ArrayList;
import java.util.Iterator;


public class gamll {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> textFile = sc.textFile("data/user_visit_action.txt");
         textFile
                .filter(v -> {
                    String[] split = v.split("_");
                    return "null".equals(split[5]);
                })
                .flatMap(v -> {
                    String[] split = v.split("_");
                    ArrayList<HotTopN> list = new ArrayList<>();
                    if (!"-1".equals(split[6]))
                        list.add(new HotTopN(split[6], 1L, 0L, 0L));
                    else if (!"null".equals(split[8]))
                        for (int i = 0; i < split[8].split(",").length; i++)
                            list.add( new HotTopN(split[8].split(",")[i], 0L, 1L, 0L));
                    else
                        for (int i = 0; i < split[10].split(",").length; i++)
                            list.add( new HotTopN(split[10].split(",")[i], 0L, 0L, 1L));

                    return list.iterator();
                })
                 .mapToPair(n->new Tuple2<>(n.getSid(),n))
                 .reduceByKey((v1,v2)->{
                     v1.setClickCOUNT(v1.getClickCOUNT() +v2.getClickCOUNT());
                     v1.setOrderCOUNT(v1.getOrderCOUNT()+v2.getOrderCOUNT());
                     v1.setPayCOUNT(v1.getPayCOUNT() +v2.getPayCOUNT());
                     return v1;
                 })
                 .map(v->v._2)
                 .sortBy(v->v,true,2)
                 .takeOrdered(10).forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }
     public static class HotTopN implements Serializable,Comparable<HotTopN> {
        private String sid;
        private Long clickCOUNT;
        private Long orderCOUNT;
        private Long payCOUNT;

        public HotTopN() {

        }

        @Override
        public String toString() {
            return "HotTopN{" +
                    "sid='" + sid + '\'' +
                    ", clickCOUNT=" + clickCOUNT +
                    ", orderCOUNT=" + orderCOUNT +
                    ", payCOUNT=" + payCOUNT +
                    '}';
        }

        public HotTopN(String sid, Long clickCOUNT, Long orderCOUNT, Long payCOUNT) {
            this.sid = sid;
            this.clickCOUNT = clickCOUNT;
            this.orderCOUNT = orderCOUNT;
            this.payCOUNT = payCOUNT;
        }

        public String getSid() {
            return sid;
        }

        public void setSid(String sid) {
            this.sid = sid;
        }

        public Long getClickCOUNT() {
            return clickCOUNT;
        }

        public void setClickCOUNT(Long clickCOUNT) {
            this.clickCOUNT = clickCOUNT;
        }

        public Long getOrderCOUNT() {
            return orderCOUNT;
        }

        public void setOrderCOUNT(Long orderCOUNT) {
            this.orderCOUNT = orderCOUNT;
        }

        public Long getPayCOUNT() {
            return payCOUNT;
        }

        public void setPayCOUNT(Long payCOUNT) {
            this.payCOUNT = payCOUNT;
        }

        @Override
        public int compareTo(HotTopN o) {
            if (this.clickCOUNT > o.clickCOUNT)
                return -1;
            else  if (this.clickCOUNT < o.clickCOUNT)
                return 1;
            else if(this.orderCOUNT > o.orderCOUNT)
                return -1;
            else if (this.orderCOUNT < o.orderCOUNT)
                return 1;
            else return (int)(this.payCOUNT - o.payCOUNT);

        }

    }
}
