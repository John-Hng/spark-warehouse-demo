package com.kirk.warehouse.util;

import com.mysql.cj.result.Row;
import org.apache.spark.sql.Dataset;

public final class DataQualityUtil {
    private DataQualityUtil(){}

    public static long checkUnique(Dataset<Row> df, String... primarykeys){
        long total = df.count();
        long count = df.dropDuplicates(primarykeys).count();
        return total - count;
    }

    public static long checkNotNull(Dataset<Row> df,String field){
        return df.filter(df.col(field).isNull()).count();
    }
}
