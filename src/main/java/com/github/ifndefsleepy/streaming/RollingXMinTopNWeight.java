package com.github.ifndefsleepy.streaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
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
                "    TO_TIMESTAMP(MAX(ts), 'yyyy-MM-dd HH:mm:ss') AS ts,\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
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
                "    h.total_volume * p.lastPrice AS holding_value," +
                "    h.ts\n" +
                "FROM current_holdings h\n" +
                "LEFT JOIN price FOR SYSTEM_TIME AS OF h.ts p\n" +
                "ON h.instrumentId = p.instrumentId";

        String minuteSnapshotValueSql = "" +
                "CREATE TEMPORARY VIEW minute_snapshots AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    instrumentId,\n" +
                "    window_end AS snapshot_time,\n" +
                "    LAST_VALUE(holding_value) AS holding_value_snapshot\n" +
                "FROM TABLE(\n" +
                "    CUMULATE(\n" +
                "        TABLE holding_values,\n" +
                "        DESCRIPTOR(ts),\n" +
                "        INTERVAL '1' MINUTE,\n" +
                "        INTERVAL '1' DAY\n" +
                "    ))\n" +
                "GROUP BY accountId, instrumentId, window_end";

        String top10MinuteSnapshotSql = "" +
                "CREATE TEMPORARY VIEW top10_instrument AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    instrumentId,\n" +
                "    window_end AS snapshot_time,\n" +
                "    holding_value_snapshot\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        accountId,\n" +
                "        instrumentId,\n" +
                "        window_end,\n" +
                "        holding_value_snapshot,\n" +
                "        ROW_NUMBER() OVER (\n" +
                "            PARTITION BY accountId, snapshot_time\n" +
                "            ORDER BY holding_value_snapshot DESC\n" +
                "        ) AS rank_num\n" +
                "    FROM TABLE (\n" +
                "        TUMBLE (\n" +
                "            TABLE minute_snapshots,\n" +
                "            DESCRIPTOR(snapshot_time),\n" +
                "            INTERVAL '1' MINUTE\n" +
                "        )\n" +
                "    )\n" +
                ")\n" +
                "WHERE rank_num <= 10";

        String marketValueSql = "" +
                "CREATE TEMPORARY VIEW market_value AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    window_end AS snapshot_time,\n" +
                "    SUM(holding_value_snapshot) AS market_value\n" +
                "FROM TABLE(\n" +
                "    TUMBLE(\n" +
                "        TABLE minute_snapshots,\n" +
                "        DESCRIPTOR(snapshot_time),\n" +
                "        INTERVAL '1' MINUTE\n" +
                "    ))\n" +
                "GROUP BY accountId, window_end";

        String top10WeightSql = "" +
                "CREATE TEMPORARY VIEW top_10_weight AS\n" +
                "SELECT\n" +
                "    t.accountId,\n" +
                "    t.instrumentId,\n" +
                "    t.snapshot_time AS snapshot_time,\n" +
                "    SUM(t.holding_value_snapshot) / m.market_value AS Top10Weight\n" +
                "FROM top10_instrument t\n" +
                "INNER JOIN market_value m \n" +
                "    ON t.accountId = m.accountId\n" +
                "    AND t.snapshot_time BETWEEN m.snapshot_time - INTERVAL '1' SECOND \n" +
                "                            AND m.snapshot_time + INTERVAL '1' SECOND\n" +
                "GROUP BY t.accountId, t.instrumentId, t.snapshot_time, m.market_value";

        String rolling60MinTop10WeightSql = "" +
                "CREATE TEMPORARY VIEW rolling_60_min_top_10_weight AS\n" +
                "SELECT\n" +
                "    accountId,\n" +
                "    window_end AS ts,\n" +
                "    AVG(Top10Weight) AS rolling60MinTop10Weight\n" +
                "FROM TABLE (\n" +
                "    TUMBLE (\n" +
                "        TABLE top_10_weight,\n" +
                "        DESCRIPTOR(snapshot_time),\n" +
                "        INTERVAL '60' MINUTES\n" +
                "    ))\n" +
                "GROUP BY accountId, window_end";

        String rolling60MinMarketValueSql = "" +
                "CREATE TEMPORARY VIEW rolling_60_min_market_value AS\n" +
                "SELECT \n" +
                "    accountId,\n" +
                "    window_end AS ts,\n" +
                "    SUM(market_value) AS rolling60MinMarketValue \n" +
                "FROM TABLE(\n" +
                "    TUMBLE(\n" +
                "        TABLE market_value,\n" +
                "        DESCRIPTOR(snapshot_time),\n" +
                "        INTERVAL '60' MINUTES\n" +
                "    ))\n" +
                "GROUP BY accountId, window_end";

        String rolling60MinTop10WeightPrintTable = "" +
                "CREATE TABLE rolling_60_min_top_10_weight_print (\n" +
                "   accountId INT,\n" +
                "   ts TIMESTAMP,\n" +
                "   rolling60MinTop10Weight DOUBLE\n" +
                ") WITH ('connector' = 'print')";

        String rolling60MinMarketValueTable = "" +
                "CREATE TABLE rolling_60_min_market_value_print (\n" +
                "   accountId INT,\n" +
                "   ts TIMESTAMP,\n" +
                "   rolling60MinMarketValue DOUBLE\n" +
                ") WITH ('connector' = 'print')";

        tableEnv.executeSql(createStockHoldingTableSql).await();
        tableEnv.executeSql(createPriceTableSql).await();
        tableEnv.executeSql(createTradeTableSql).await();
        tableEnv.executeSql(rolling60MinTop10WeightPrintTable).await();
        tableEnv.executeSql(rolling60MinMarketValueTable).await();

        tableEnv.executeSql(currentHoldingSql).await();
        tableEnv.executeSql(currentHoldingValueSql).await();
        tableEnv.executeSql(minuteSnapshotValueSql).await();
        tableEnv.executeSql(top10MinuteSnapshotSql).await();
        tableEnv.executeSql(marketValueSql).await();
        tableEnv.executeSql(top10WeightSql).await();
        tableEnv.executeSql(rolling60MinTop10WeightSql).await();
        tableEnv.executeSql(rolling60MinMarketValueSql).await();

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("" +
                "INSERT INTO rolling_60_min_top_10_weight_print\n" +
                "SELECT\n" +
                "   accountId,\n" +
                "   ts,\n" +
                "   rolling60MinTop10Weight\n" +
                "FROM rolling_60_min_top_10_weight\n");

        statementSet.addInsertSql("" +
                "INSERT INTO rolling_60_min_market_value_print\n" +
                "SELECT\n" +
                "   accountId,\n" +
                "   ts,\n" +
                "   rolling60MinMarketValue\n" +
                "FROM rolling_60_min_market_value");

        String executeSql = "" +
                "INSERT INTO rolling_60_min_top_10_weight_print\n" +
                "SELECT\n" +
                "   accountId,\n" +
                "   ts,\n" +
                "   rolling60MinTop10Weight\n" +
                "FROM rolling_60_min_top_10_weight;\n" +
                "\n" +
                "INSERT INTO rolling_60_min_market_value_print\n" +
                "SELECT\n" +
                "   accountId,\n" +
                "   ts,\n" +
                "   rolling60MinMarketValue\n" +
                "FROM rolling_60_min_market_value";
        statementSet.execute();
        env.execute();
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
