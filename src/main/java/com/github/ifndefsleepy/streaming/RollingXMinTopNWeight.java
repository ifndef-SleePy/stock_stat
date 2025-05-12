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
                "            ELSE 0\n" +
                "        END\n" +
                "    ) AS total_volume,\n" +
                "    TO_TIMESTAMP(MAX(ts), 'yyyy-MM-dd HH:mm:ss') AS ts\n" +
                "FROM (\n" +
                "    SELECT accountId, instrumentId, 'Long' AS direction, volume, DATE_FORMAT(FLOOR(CURRENT_TIMESTAMP TO DAY) + INTERVAL '9' HOUR + INTERVAL '30' MINUTE, 'yyyy-MM-dd HH:mm:ss') AS ts \n" +
                "    FROM stock_holding\n" +
                "    UNION ALL\n" +
                "    SELECT accountId, instrumentId, direction, tradeVolume AS volume, ts \n" +
                "    FROM trade\n" +
                ")\n" +
                "GROUP BY accountId, instrumentId";

        String currentHoldingValueSql = "" +
                "CREATE VIEW holding_values AS\n" +
                "SELECT \n" +
                "    h.accountId,\n" +
                "    h.instrumentId,\n" +
                "    h.total_volume * p.lastPrice AS holding_value\n" +
                "FROM current_holdings h\n" +
                "LEFT JOIN price FOR SYSTEM_TIME AS OF h.ts p\n" +
                "ON h.instrumentId = p.instrumentId";

        String minuteSnapshotValueSql = "" +
                "CREATE TEMPORARY VIEW minute_snapshots AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    window_end AS snapshot_time,\n" +
                "    instrumentId,\n" +
                "    LAST_VALUE(holding_value) AS holding_value_snapshot,\n" +
                "    SUM(holding_value) AS total_value\n" +
                "FROM TABLE(\n" +
                "    CUMULATE(\n" +
                "        TABLE holding_values,\n" +
                "        DESCRIPTOR(ts),\n" +
                "        INTERVAL '1' MINUTE,\n" +
                "        INTERVAL '1' DAY\n" +
                "    ))\n" +
                "GROUP BY accountId";

        String top10WeightSql = "" +
                "CREATE TEMPORARY VIEW top_10_weights AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    window_end AS ts,\n" +
                "    AVG(holding_weight) AS rolling60minTop10Weight,\n" +
                "    AVG(total_value) AS rollingXMinMarketValue\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        accountId,\n" +
                "        window_end,\n" +
                "        snapshot_time,\n" +
                "        (holding_value_snapshot / total_value) AS holding_weight,\n" +
                "        LAST_VALUE(total_value) AS total_value\n" +
                "    FROM (\n" +
                "        SELECT *,\n" +
                "            ROW_NUMBER() OVER (\n" +
                "                PARTITION BY accountId, snapshot_time, snapshotTime\n" +
                "                ORDER BY (holding_value_snapshot / total_value) DESC\n" +
                "            ) AS rank\n" +
                "        FROM TABLE(\n" +
                "            HOP(\n" +
                "                TABLE minute_snapshots,\n" +
                "                DESCRIPTOR(snapshot_time),\n" +
                "                INTERVAL '1' MINUTE,\n" +
                "                INTERVAL '60' MINUTES\n" +
                "            )\n" +
                "        )\n" +
                "    )\n" +
                "    WHERE rank <= 10\n" +
                "    GROUP BY accountId, window_end, snapshotTime\n" +
                ")\n" +
                "GROUP BY accountId, snapshot_time;";

        String printTableSql = "" +
                "CREATE TABLE print (\n" +
                "   accountId STRING,\n" +
                "   ts TIMESTAMP,\n" +
                "   rolling60minTop10Weight DOUBLE,\n" +
                "   rollingXMinMarketValue DOUBLE\n" +
                ") WITH ('connector' = 'print')";

        String executeSql = "" +
                "INSERT INTO print\n" +
                "SELECT\n" +
                "   t.accountId,\n" +
                "   t.ts,\n" +
                "   t.rolling60minTop10Weight,\n" +
                "   t.rollingXMinMarketValue\n" +
                "FROM top_10_weights t";

        tableEnv.executeSql(createStockHoldingTableSql).await();
        tableEnv.executeSql(createPriceTableSql).await();
        tableEnv.executeSql(createTradeTableSql).await();
        tableEnv.executeSql(printTableSql).await();

        tableEnv.executeSql(currentHoldingSql).await();
        tableEnv.executeSql(currentHoldingValueSql).await();
        tableEnv.executeSql(minuteSnapshotValueSql).await();
        tableEnv.executeSql(top10WeightSql).await();

        tableEnv.executeSql(executeSql);
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
