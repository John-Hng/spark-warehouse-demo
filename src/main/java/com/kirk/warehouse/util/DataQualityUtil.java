package com.kirk.warehouse.util;

import org.apache.spark.sql.SparkSession;

public final class DataQualityUtil {
    private DataQualityUtil(){}

    public static boolean checkPartitionNotEmpty(SparkSession spark,String table,String dt) {
        String sql = String.format(
                "select count(1) as cnt from %s where dt = '%s'",
                table,dt
        );

        long cnt = spark.sql(sql).first().getAs("cnt");

        if (cnt > 0) {
            System.out.printf("[数据质量-分区校验] %s 分区 %s 通过，数据量：%d 条\n",table,dt,cnt);
            return true;
        } else {
            System.err.printf("[数据质量-分区校验] 失败：%s 分区 %s 数据为空！\n",table,dt);
            return false;
        }
    }

    public static boolean checkPrimaryKeyUnique(SparkSession spark,String table,String dt,String primaryKey) {
        String sql = String.format(
                "select\n" +
                "\tcount(1) as duplicate_cnt\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\t%s,\n" +
                "\t\tcount(1) as cnt\n" +
                "\tfrom %s\n" +
                "\twhere dt = '%s'\n" +
                "\tgroup by %s having cnt > 1\n" +
                ") t",
                primaryKey,table,dt,primaryKey
        );

        long cnt = spark.sql(sql).first().getAs("duplicate_cnt");

        if (cnt == 0){
            System.out.printf("[数据质量-主键校验] %s 分区 %s 通过，主键无重复\n",table,dt);
            return true;
        } else {
            System.err.printf("[数据质量-主键校验] 失败：%s 分区 %s 存在 %d 条重复主键！\n",table,dt,cnt);
            return false;
        }

    }

    public static boolean checkColumnNull(SparkSession spark, String tableName, String dt, String columns, double maxNullRatio) {
        String[] colArray = columns.split(",");
        StringBuilder caseBuilder = new StringBuilder();
        for (int i = 0; i < colArray.length; i++) {
            if (i > 0) caseBuilder.append(" + ");
            caseBuilder.append(String.format("CASE WHEN %s IS NULL OR %s = '' THEN 1 ELSE 0 END", colArray[i], colArray[i]));
        }

        String sql = String.format(
                "SELECT " +
                        "  COUNT(*) AS total, " +
                        "  SUM(%s) AS null_cnt " +
                        "FROM %s WHERE dt = '%s'",
                caseBuilder, tableName, dt
        );

        long total = spark.sql(sql).first().getLong(0);
        long nullCnt = spark.sql(sql).first().getLong(1);
        double nullRatio = total == 0 ? 0 : (double) nullCnt / total;

        if (nullRatio <= maxNullRatio) {
            System.out.printf("[数据质量-空值校验] %s 分区 %s 通过，空值占比：%.2f%%\n",
                    tableName, dt, nullRatio * 100);
            return true;
        } else {
            System.err.printf("[数据质量-空值校验] 失败：%s 分区 %s 空值占比 %.2f%%，超过阈值 %.2f%%\n",
                    tableName, dt, nullRatio * 100, maxNullRatio * 100);
            return false;
        }

    }

    public static boolean checkRowCountRatio(SparkSession spark, String upstreamTable, String currentTable,
                                             String dt, double minRate, double maxRate) {
        long upstreamCnt = spark.sql(
                String.format("SELECT COUNT(*) FROM %s WHERE dt = '%s'", upstreamTable, dt)
        ).first().getLong(0);

        long currentCnt = spark.sql(
                String.format("SELECT COUNT(*) FROM %s WHERE dt = '%s'", currentTable, dt)
        ).first().getLong(0);

        if (upstreamCnt == 0) {
            System.err.println("[数据质量-波动校验] 失败：上游表数据量为0，无法对比");
            return false;
        }

        double ratio = (double) currentCnt / upstreamCnt;

        if (ratio >= minRate && ratio <= maxRate) {
            System.out.printf("[数据质量-波动校验] %s -> %s 分区 %s 通过，数据量比例：%.2f%%\n",
                    upstreamTable, currentTable, dt, ratio * 100);
            return true;
        } else {
            System.err.printf("[数据质量-波动校验] 失败：%s -> %s 分区 %s 数据量比例 %.2f%%，超出区间 [%.0f%%, %.0f%%]\n",
                    upstreamTable, currentTable, dt, ratio * 100, minRate * 100, maxRate * 100);
            return false;
        }
    }

    public static boolean checkEnumValue(SparkSession spark, String tableName, String dt,
                                         String column, String validValues) {
        String sql = String.format(
                "SELECT COUNT(*) AS invalid_cnt FROM %s " +
                        "WHERE dt = '%s' AND %s NOT IN ('%s')",
                tableName, dt, column, validValues.replace(",", "','")
        );
        long invalidCnt = spark.sql(sql).first().getLong(0);

        if (invalidCnt == 0) {
            System.out.printf("[数据质量-枚举校验] %s 分区 %s 字段 %s 通过\n", tableName, dt, column);
            return true;
        } else {
            System.err.printf("[数据质量-枚举校验] 失败：%s 分区 %s 字段 %s 存在 %d 条非法值\n",
                    tableName, dt, column, invalidCnt);
            return false;
        }
    }
}
