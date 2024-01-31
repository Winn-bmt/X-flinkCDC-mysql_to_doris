package com.winn.sync.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description:【需手动配置】 mysqlSource配置
 */
public class MySqlUtils {

    //mysql数据库配置
    public static final String HOST = "ip地址";
    public static final int PORT = 3306;
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static Properties getProp(){
        Properties prop = new Properties();
        // 1.Mysql_debeziumProperties
            //时间转换器参数
        prop.setProperty("converters","dbziumConverter");
        prop.setProperty("dbziumConverter.type", "com.winn.sync.utils.DtToStrConvertor");
        prop.setProperty("dbziumConverter.format.date", "yyyy-MM-dd");
        prop.setProperty("dbziumConverter.format.time", "HH:mm:ss");
        prop.setProperty("dbziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        prop.setProperty("dbziumConverter.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        prop.setProperty("dbziumConverter.format.timestamp.zone", "UTC+8");
            //数值类型映射
        prop.setProperty("bigint.unsigned.handling.mode","long");
        prop.setProperty("decimal.handling.mode","String");
        // 2.Mysql_jdbcProperties
        prop.setProperty("autoReconnect","true");
        prop.setProperty("useSSL", "false");
        return prop;
    }

    //desc 获取mysqlCDCSource
    public static MySqlSource<String> getSrc(String dbName, String fullTableName) {
        return MySqlSource.<String>builder()
                .databaseList(dbName)
                .tableList(fullTableName)
                .hostname(HOST)
                .port(PORT)
                .username(USERNAME)
                .password(PASSWORD)
                .jdbcProperties(getProp())
                .debeziumProperties(getProp())
                .startupOptions(StartupOptions.initial())
                .scanNewlyAddedTableEnabled(true)//开启扫描新添加表功能
                .deserializer(new JsonDebeziumDeserializationSchema())
                .closeIdleReaders(true)
                .splitSize(100000)//每块有多少行
                .fetchSize(20000)//每次抓取的行数
                .connectionPoolSize(40)//连接池大小20
                .connectMaxRetries(5)//失败重试5次
                .connectTimeout(Duration.ofSeconds(600L))//超时时间10分钟
                .build();
    }
}
