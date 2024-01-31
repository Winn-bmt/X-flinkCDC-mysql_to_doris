package com.winn.sync.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description: 【需手动配置】dorisSink配置
 */
public class DorisUtils {

    public static final String DORIS_FE = "host:port";//doris数据库的fe节点的 ip:端口
    public static final String DORIS_DB = "test";//写入doris的数据库名
    public static final String USER_NAME = "username";//doris数据库的用户名
    public static final String PASSWORD = "password";//doris数据库的用户密码

    public static Properties getProp(String hiddenColumns){
        Properties prop = new Properties();
        prop.setProperty("format","json");
        prop.setProperty("read_json_by_line","true");
        prop.setProperty("hidden_columns", hiddenColumns);
        return prop;
    }

    public static DorisSink<String> getSink(String tableName, String hiddenColumns){
        DorisOptions dorisOpt = DorisOptions.builder()
                .setFenodes(DORIS_FE)
                .setTableIdentifier(DORIS_DB +"." + tableName)
                .setUsername(USER_NAME)
                .setPassword(PASSWORD)
                .build();

        DorisExecutionOptions dorisExOpt = DorisExecutionOptions.builder()
                .disable2PC()//禁用2pc
                .setLabelPrefix("DB")//配置写入doris任务的标签
                .setDeletable(true)
                .setStreamLoadProp(getProp(hiddenColumns))
                .setMaxRetries(5)
/*             .setCheckInterval(20000)
                .setBufferSize(1024*1024*2)
                .setBufferCount(5)*///流式写入参数配置
                .setBatchMode(true)
                .setBufferFlushMaxRows(100000)
                .setBufferFlushMaxBytes(20*1024*1024)
                .setBufferFlushIntervalMs(TimeUnit.SECONDS.toMillis(10))
                .setFlushQueueSize(2)
                .setUseCache(true)
                .build();

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(dorisOpt)
                .setDorisExecutionOptions(dorisExOpt)
                .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOpt).build())
                .build();
    }
}
