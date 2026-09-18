package com.kirk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf();
        conf.setAppName("spark");
        conf.setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.close();
    }
}
