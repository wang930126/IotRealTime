package controller.dws;

import bean.DimensionStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.dwm.DimAsyncFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.MyKafkaUtil;

import java.util.concurrent.TimeUnit;

/**
 * 尺寸主题宽表计算：
 */
public class DimensionStatsApp {

    public static String groupId = DimensionStatsApp.class.getSimpleName();

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(4);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/dw"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        System.setProperty("HADOOP_USER_NAME", "wang930126");

        // TODO 维度数据【生产id】 -> [工厂id，产线id，生产设备，工艺id，工艺类型]
        // TODO 1.0 获取质量检验明细流(dwd) 【检验明细id，尺寸id，生产id，是否是近期新加入的尺寸，合格？】
        String dwd_inspect_detail_topic = "dwd_inspect_detail_topic";
        FlinkKafkaConsumer<String> c1 = MyKafkaUtil.getKafkaConsumer(dwd_inspect_detail_topic, groupId);
        DataStreamSource<String> inspectDetailStream = env.addSource(c1);

        // TODO 1.1 获取独立尺寸检验明细流(dwm) 【检验明细id，尺寸id，生产id，是否是近期新加入的尺寸，合格？】
        String dwm_inspect_per_dimension_topic = "dwm_inspect_per_dimension_topic";
        FlinkKafkaConsumer<String> c2 = MyKafkaUtil.getKafkaConsumer(dwm_inspect_per_dimension_topic, groupId);
        DataStreamSource<String> inspectDetailPerDimStream = env.addSource(c2);

        // TODO 1.2 获取复检尺寸明细流(dwm) 【检验明细id，尺寸id，生产id，是否是近期新加入的尺寸，合格？】
        String dwm_inspect_double_topic = "dwm_inspect_double_topic";
        FlinkKafkaConsumer<String> c3 = MyKafkaUtil.getKafkaConsumer(dwm_inspect_double_topic, groupId);
        DataStreamSource<String> inspectDetailDoubleCheckDimensionStream = env.addSource(c3);

        // TODO 2.转换上面获取的流到一个统一的bean对象中
        // 缺陷尺寸总数 检验总尺寸数 缺陷尺寸总种数 复检尺寸总数 复检不合格尺寸总数
        // 窗口开始时间 窗口结束时间 工厂维度 产线维度 工艺类型维度 生产设备维度 是否是新尺寸
        SingleOutputStreamOperator<DimensionStats> stat1 = inspectDetailStream.map(new MapFunction<String, DimensionStats>() {
            @Override
            public DimensionStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
//                "1".equals(jsonObj.getString("isPass")) ? 0L : 1L;
                DimensionStats stat = new DimensionStats(
                        "",
                        "",
                        jsonObj.getString("mid"),
                        "",
                        "",
                        "",
                        "",
                        jsonObj.getString("isNew"),
                        "1".equals(jsonObj.getString("isPass")) ? 0L : 1L,
                        1L,
                        0L,
                        0L,
                        0L
                );
                return stat;
            }
        });


        SingleOutputStreamOperator<DimensionStats> stat2 = inspectDetailPerDimStream.map(new MapFunction<String, DimensionStats>() {
            @Override
            public DimensionStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                DimensionStats stat = new DimensionStats(
                        "", //开窗的时候计算窗口开始和结束时间
                        "",
                        jsonObj.getString("mid"), //通过制造id获取其余维度值
                        "",
                        "",
                        "",
                        "",
                        jsonObj.getString("isNew"),
                        0l,
                        0l,
                        1L,
                        0L,
                        0L
                );
                return stat;
            }
        });

        SingleOutputStreamOperator<DimensionStats> stat3 = inspectDetailDoubleCheckDimensionStream.map(new MapFunction<String, DimensionStats>() {
            @Override
            public DimensionStats map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                DimensionStats stat = new DimensionStats(
                        "", //开窗的时候计算窗口开始和结束时间
                        "",
                        jsonObj.getString("mid"), //通过制造id获取其余维度值
                        "",
                        "",
                        "",
                        "",
                        jsonObj.getString("isNew"),
                        0l,
                        0l,
                        0L,
                        1L,
                        "1".equals(jsonObj.getString("isPass")) ? 0L : 1L
                );
                return stat;
            }
        });

        // TODO 3.合并上面的几条流
        DataStream<DimensionStats> dimensionStatsStream = stat1.union(stat2, stat3);

        // TODO 4.缺失一些维度数据的信息 去HBase中异步查找
        SingleOutputStreamOperator<DimensionStats> withDimInfo = AsyncDataStream.unorderedWait(
                dimensionStatsStream,
                new DimAsyncFunction<DimensionStats>("DIM_ShengChan") {
                    @Override
                    public void join(DimensionStats stat, JSONObject obj) {
                        stat.seteId(obj.getString("eId"));
                        stat.setFid(obj.getString("Fid"));
                        stat.setmType(obj.getString("mType"));
                        stat.setLid(obj.getString("lid"));
                    }

                    @Override
                    public String getKeyId(DimensionStats stat) {
                        return stat.getMid();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 5.由于要开窗 设置水位线
        SingleOutputStreamOperator<DimensionStats> withWaterMark = withDimInfo.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<DimensionStats>(Time.of(3, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(DimensionStats element) {
                        return element.getTs();
                    }
                }
        );

        // TODO 6.分组



    }

}
