package com.github.ifndefsleepy.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class TopNWeight {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String dt = "2025-05-09";

        Path stackValuesFileDir = saveResourceToTmpDir("batch/stock_values.csv", "dt=" + dt);
        Path priceFileDir = saveResourceToTmpDir("batch/price.csv", "dt=" + dt);

        String createStockHoldingTableSql = "" +
                "CREATE TABLE stock_holding (\n" +
                "   accountId INT,\n" +
                "   instrumentId STRING,\n" +
                "   direction STRING,\n" +
                "   volume INT,\n" +
                "   dt STRING\n" +
                ")\n" +
                "PARTITIONED BY (dt)\n" + // 离线数仓一般以天为分区存储长期数据
                "WITH (\n" +
                "   'connector'='filesystem',\n" + // 简化为文件以方便构造测试数据
                "   'path'='file://" + stackValuesFileDir + "',\n" +
                "   'format'='csv'\n" +
                ")";

        String createPriceTableSql = "" +
                "CREATE TABLE price (\n" +
                "   instrumentId STRING,\n" +
                "   lastPrice DOUBLE,\n" +
                "   ts STRING,\n" + // 简化为 STRING 以方便构造测试数据
                "   dt STRING\n" + // 一般离线数仓会按天存储
                ")\n" +
                "PARTITIONED BY (dt)\n" + // 离线数仓一般以天为分区存储长期数据
                "WITH (\n" +
                "   'connector'='filesystem',\n" + // 简化为文件以方便构造测试数据
                "   'path'='file://" + priceFileDir + "',\n" +
                "   'format'='csv'\n" +
                ")";

        String topNWeightSql = "" +
                "WITH \n" +
                "latest_prices AS (\n" +
                "    SELECT \n" +
                "        instrumentId,\n" +
                "        dt,\n" +
                "        lastPrice\n" +
                "    FROM (\n" + // price 表按 dt 取当天最后报价
                "        SELECT \n" +
                "            instrumentId,\n" +
                "            dt,\n" +
                "            lastPrice,\n" +
                "            ROW_NUMBER() OVER (\n" +
                "                PARTITION BY instrumentId, dt\n" +
                "                ORDER BY ts DESC\n" +
                "            ) AS rn\n" +
                "        FROM price\n" +
                "        WHERE dt = '" + dt + "'\n" +
                "    ) t\n" +
                "    WHERE rn = 1\n" +
                ")," +
                "stock_values AS (\n" +
                "    SELECT \n" +
                "        s.accountId,\n" +
                "        s.instrumentId,\n" +
                "        s.volume * p.lastPrice AS `value`\n" +
                "    FROM stock_holding s\n" +
                "    INNER JOIN latest_prices p \n" +
                "        ON s.instrumentId = p.instrumentId\n" +
                "        AND s.dt = p.dt\n" +
                "    WHERE s.dt= '" + dt + "'\n" +
                "),\n" +
                "account_totals AS (\n" +
                "    SELECT \n" +
                "        accountId,\n" +
                "        SUM(`value`) AS total_value\n" +
                "    FROM stock_values\n" +
                "    GROUP BY accountId\n" +
                "),\n" +
                "account_topN AS (\n" +
                "    SELECT \n" +
                "        accountId,\n" +
                "        SUM(`value`) AS topN_value\n" +
                "    FROM (\n" +
                "        SELECT \n" +
                "            accountId,\n" +
                "            `value`,\n" +
                "            ROW_NUMBER() OVER (\n" +
                "                PARTITION BY accountId \n" +
                "                ORDER BY `value` DESC\n" +
                "            ) AS `rank`\n" +
                "        FROM stock_values\n" +
                "        ) ranked\n" +
                "    WHERE `rank` <= 2\n" + // 以 top 2 为例
                "    GROUP BY accountId\n" +
                ")\n" +
                "SELECT \n" +
                "    '" + dt + "' AS dt," +
                "    t.accountId,\n" +
                "    a.topN_value / t.total_value AS topNWeight,\n" +
                "    t.total_value AS marketValue\n" +
                "FROM account_totals t\n" +
                "INNER JOIN account_topN a \n" +
                "    ON t.accountId = a.accountId";

        tableEnv.executeSql(createStockHoldingTableSql).await();
        tableEnv.executeSql(createPriceTableSql).await();

        TableResult tableResult = tableEnv.executeSql(topNWeightSql);
        tableResult.print();
    }

    public static Path saveResourceToTmpDir(String resourceFile, String partition) throws IOException {
        try (InputStream is = TopNWeight.class.getResourceAsStream("/" + resourceFile)) {
            if (is == null) {
                throw new RuntimeException("File not found in resources");
            }
            Path dir = Files.createTempDirectory(null);
            Path partitionDir = dir.resolve(partition);
            Files.createDirectories(partitionDir);
            String fileName = resourceFile.substring(resourceFile.lastIndexOf("/") + 1);
            Path tmpFile = partitionDir.resolve(fileName);
            Files.copy(is, tmpFile, StandardCopyOption.REPLACE_EXISTING);
            return dir;
        }
    }
}
