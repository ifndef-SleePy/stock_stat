package com.github.ifndefsleepy.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNWeight {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createStockHoldingTableSql = "" +
                "CREATE TABLE stock_holding (" +
                "   accountId INT," +
                "   instrumentId STRING," +
                "   direction STRING," +
                "   volume INT," +
                "   dt STRING," +
                "   PRIMARY KEY (accountId, instrumentId) NOT ENFORCED" + // TODO
                ") " +
                "PARTITION BY (dt)" +
                "WITH (" +
                "   'connector'='filesystem'," + // replace with real connector, like Hive, Hudi, Iceberg, Paimon...
                "   'path'=''," + // TODO
                "   'format'='csv'," +
                "   ''" +
                ")";

        String createPriceTableSql = "" +
                "CREATE TABLE price (" +
                "   instrumentId STRING," +
                "   lastPrice DOUBLE," +
                "   ts TIMESTAMP," +
                "   PRIMARY KEY " + // TODO
                ")";
    }
}
