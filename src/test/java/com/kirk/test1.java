package com.kirk;

import org.apache.spark.sql.SparkSession;
import com.kirk.warehouse.util.SparkSessionUtil;

import java.util.Properties;

public class test1 {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession test = SparkSessionUtil.getSession("test");

        //test.sql("create table test(id int,name string,age int);");

        test.sql("select * from dws.dws_user_behavior_day where dt = '2017-11-27'").show();
    }
}
