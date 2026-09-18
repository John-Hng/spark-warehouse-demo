package com.kirk;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class test {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark")
                .getOrCreate();
        Dataset<Row> csv = sparkSession.read().csv("C:\\Users\\34961\\Desktop\\UserBehavior.csv");

        csv.createOrReplaceTempView("t1");

        sparkSession.sql("select * from t1 limit 100").write().csv("output");
    }
}
