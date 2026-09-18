package com.kirk.warehouse.util;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionUtil {
    private SparkSessionUtil(){}

    public static SparkSession getSession (String appName) {
        return SparkSession
                .builder()
                .appName(appName)
                .master("yarn")
                .enableHiveSupport()
                .getOrCreate();
    }
}
