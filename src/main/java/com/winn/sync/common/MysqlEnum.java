package com.winn.sync.common;

import com.winn.sync.utils.MySqlUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description: 【需手动配置】配置mysql数据源的列表
 */
public enum MysqlEnum {

    /**
     * 将需要读取的表信息，以枚举的形式在下面列出即可
     * @param sourceDBName 表的数据库名
     * @param sourceTableName 表名
     */
    TABLENAME_1("DBName_1", "tableName"),
    TABLENAME_2("DBName-2", "tableName");

    private final MySqlSource<String> mySqlSrc;

    MysqlEnum(String sourceDBName, String sourceTableName) {
        this.mySqlSrc = MySqlUtils.getSrc(sourceDBName,sourceDBName + "." + sourceTableName);
    }

    public MySqlSource<String> getSrc() {
        return mySqlSrc;
    }
}
