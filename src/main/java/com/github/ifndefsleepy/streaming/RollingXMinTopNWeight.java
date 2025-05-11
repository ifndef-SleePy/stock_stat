package com.github.ifndefsleepy.streaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class RollingXMinTopNWeight {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String dt = "2025-05-09";

        Path stackValuesFileDir = saveResourceToTmpDir("streaming/stock_values.csv");
        Path tradeFileDir = saveResourceToTmpDir("streaming/trade.csv", "dt=" + dt);
        Path priceFileDir = saveResourceToTmpDir("streaming/price.csv");

        String createStockHoldingTableSql = "" +
                "CREATE TABLE stock_holding (\n" +
                "   accountId INT,\n" +
                "   instrumentId STRING,\n" +
                "   direction STRING,\n" +
                "   volume INT\n" +
                ")\n" +
                "WITH (\n" +
                "   'connector'='filesystem',\n" + // 简化为文件以方便构造测试数据
                "   'path'='file://" + stackValuesFileDir + "',\n" +
                "   'format'='csv'\n" +
                ")";

        String createPriceTableSql = "" +
                "CREATE TABLE price (\n" +
                "   instrumentId STRING,\n" +
                "   lastPrice DOUBLE,\n" +
                "   ts STRING\n" + // 简化为 STRING 以方便构造测试数据
                ")\n" +
                "WITH (\n" +
                "   'connector'='filesystem',\n" + // 简化为文件以方便构造测试数据
                "   'path'='file://" + priceFileDir + "',\n" +
                "   'format'='csv'\n" +
                ")";

        String createTradeTableSql = "" +
                "CREATE TABLE trade (\n" +
                "   accountId INT,\n" +
                "   orderId BIGINT,\n" +
                "   instrumentId STRING,\n" +
                "   direction STRING,\n" +
                "   offsetFlag STRING,\n" +
                "   tradePrice DOUBLE,\n" +
                "   tradeVolume INT,\n" +
                "   ts STRING\n" + // 简化为 STRING 以方便构造测试数据
                ")\n" +
                "WITH (\n" +
                "   'connector'='filesystem',\n" + // 简化为文件以方便构造测试数据
                "   'path'='file://" + tradeFileDir + "',\n" +
                "   'format'='csv'\n" +
                ")";

        String currentHoldingSql = "" +
                "CREATE VIEW current_holdings AS\n" +
                "SELECT \n" +
                "    accountId,\n" +
                "    instrumentId,\n" +
                "    SUM(\n" +
                "        CASE direction \n" +
                "            WHEN 'Long' THEN volume \n" +
                "            WHEN 'Short' THEN -volume \n" +
                "        END\n" +
                "    ) AS total_volume\n" +
                "FROM (\n" +
                "    SELECT accountId, instrumentId, 'Long' AS direction, volume\n" +
                "    FROM stock_holding\n" +
                "    UNION ALL\n" +
                "    SELECT accountId, instrumentId, direction, tradeVolume \n" +
                "    FROM trade\n" +
                ")\n" +
                "GROUP BY accountId, instrumentId";

        String currentHoldingValueSql = "" +
                "CREATE VIEW holding_values AS\n" +
                "SELECT \n" +
                "    h.accountId,\n" +
                "    h.instrumentId,\n" +
                "    SUM(h.total_volume * p.lastPrice) OVER (PARTITION BY h.accountId) AS total_value\n" +
                "FROM current_holdings h\n" +
                "LEFT JOIN price FOR SYSTEM_TIME AS OF h.procTime p\n" +
                "ON h.instrumentId = sp.instrumentId";

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
        try (InputStream is = RollingXMinTopNWeight.class.getResourceAsStream("/" + resourceFile)) {
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

    public static Path saveResourceToTmpDir(String resourceFile) throws IOException {
        try (InputStream is = RollingXMinTopNWeight.class.getResourceAsStream("/" + resourceFile)) {
            if (is == null) {
                throw new RuntimeException("File not found in resources");
            }
            Path dir = Files.createTempDirectory(null);
            String fileName = resourceFile.substring(resourceFile.lastIndexOf("/") + 1);
            Path tmpFile = dir.resolve(fileName);
            Files.copy(is, tmpFile, StandardCopyOption.REPLACE_EXISTING);
            return dir;
        }
    }
}
