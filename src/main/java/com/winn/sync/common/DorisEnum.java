package com.winn.sync.common;

import com.winn.sync.utils.DorisUtils;
import org.apache.doris.flink.sink.DorisSink;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description: 【需手动配置】配置写入doris表
 */
public enum DorisEnum {

    /**
     * 配置写入doris的表，注意 Doris枚举名称要与mysql中的保存一致
     * @param sinkTableName 写入doris的表名
     * @param hiddenColumns 表中的隐藏列，写主键即可，主要是在数据更新时来进行删除的依据
     */
    TABLENAME_1("tableName_1", "id,name"),
    TABLENAME_2("tableName_2", "id");

    private final DorisSink<String> dorisSink;

    DorisEnum(String sinkTableName, String hiddenColumns) {
        this.dorisSink = DorisUtils.getSink(sinkTableName, hiddenColumns);
    }

    public DorisSink<String> getDorisSink() {
        return dorisSink;
    }

}
