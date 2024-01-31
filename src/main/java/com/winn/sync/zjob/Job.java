package com.winn.sync.zjob;

import com.winn.sync.common.DorisEnum;
import com.winn.sync.common.MysqlEnum;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description: 【无需手动配置】启动类
 */
public class Job {
    public static void main(String[] args) throws Exception {
        // DESC获取环境
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8899");// 本地测试 web ui 地址
        conf.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true);//开启火焰图
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.disableOperatorChaining();
        env.setParallelism(3);
        // 检查点
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);//开启非对齐检查点
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(1));//先使用检查点对齐，对齐超时则进行非对齐
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(1));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        HashMap<String, SingleOutputStreamOperator<String>> dsMap = new HashMap<>();

        //获取Source
        for (MysqlEnum value : MysqlEnum.values()) {
            SingleOutputStreamOperator<String> ds = env.fromSource(value.getSrc(), WatermarkStrategy.noWatermarks(), value.toString())
                    .uid("src_" + value.toString().toLowerCase())
                    .name(value.toString())
                    .setParallelism(1);
            dsMap.put(value.toString(),ds);
        }

        for (DorisEnum value : DorisEnum.values()) {
            dsMap.get(value.toString()).sinkTo(value.getDorisSink())
                    .uid("sink_" + value.toString().toLowerCase())
                    .name(value.toString())
                    .setParallelism(3);
        }
        env.execute();
    }
}
