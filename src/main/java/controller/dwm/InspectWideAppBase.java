package controller.dwm;

import bean.Equipment;
import bean.InspectDetail;
import bean.InspectInfo;
import bean.InspectWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import util.DimInfoUtil;
import util.MyKafkaUtil;
import util.RedisUtil;

import java.util.concurrent.TimeUnit;

/**
 * 用于合成检验宽表
 */
public class InspectWideAppBase {

    private static final String groupId = InspectWideAppBase.class.getSimpleName();


    public static void main(String[] args) {


    }

    /**
     * 每来一条数据 直接根据主键去Phoenix中查询数据 join
     */
    private static void rawJoin() {
        DimInfoUtil.getDimInfoNoCache("DIM_EQUIPMENT", Tuple2.of("id", "3306"));
    }

    private static void cacheJoin() {
        DataStream<InspectWide> baseStream = getBaseStream();

        String equipment_table_name = "dim_equipment";

        SingleOutputStreamOperator<InspectWide> withEquipmentStream = baseStream.map(new MapFunction<InspectWide, InspectWide>() {
            @Override
            public InspectWide map(InspectWide wide) throws Exception {
                String eid = wide.getEquipmentId();
                Jedis jedis = null;
                JSONObject equipment = null;
                try{
                    // TODO 1.去redis中查询 dim:dim_equipment:eid 这个键对应的 value是否存在
                    jedis = RedisUtil.getJedisConn();
                    // dim:DIM_EQUIPMENT:12345
                    String key = "dim:" + equipment_table_name + ":" + eid;
                    String jsonStr = jedis.get(key);

                    // TODO 2.如果存在 直接合并数据
                    if(jsonStr != null && jsonStr.length() > 0){
                        equipment = JSON.parseObject(jsonStr);
                    }else{
                        // TODO 3.不存在去HBase中查出这一条数据 放入redis缓存中
                        JSONObject equipmentObj = DimInfoUtil.getDimInfoNoCache(equipment_table_name, Tuple2.of("id", eid));
                        jedis.setex(key,60 * 24 * 60,equipmentObj.toJSONString());
                    }
                }catch (Exception e){

                }finally {
                    jedis.close();
                }


                // TODO 4.关联数据
                // merge(wide,equipment)
                return wide;
            }
        });
    }

    /**
     * 异步执行join任务
     */
    private static void asyncIOJoin(String tableName){
        DataStream<InspectWide> baseStream = getBaseStream();

        SingleOutputStreamOperator<InspectWide> withEquipmentStream = AsyncDataStream.unorderedWait(
                baseStream,
                new DimAsyncFunction<InspectWide>("DIM_EQUIPMENT") {
                    @Override
                    public void join(InspectWide input, JSONObject obj) {
                        // 根据当前ts计算出生产设备对应的工作天数
                    }

                    // 获取设备号
                    @Override
                    public String getKeyId(InspectWide input) {
                        return input.getEquipmentId();
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

    }

    private static DataStream<InspectWide> getBaseStream() {
        // TODO 0.环境初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000 * 60, CheckpointingMode.EXACTLY_ONCE);
        System.setProperty("HADOOP_USER_NAME", "wang930126");

        // TODO 1.从Kafka的DWD层中获取 InspectInfo流 和InspectDetail流
        DataStreamSource<String> dwd_inspect_info = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_inspect_info", groupId));
        SingleOutputStreamOperator<InspectInfo> inspectInfoStream = dwd_inspect_info.map(
                new MapFunction<String, InspectInfo>() {
                    @Override
                    public InspectInfo map(String value) throws Exception {
                        return JSON.parseObject(value, InspectInfo.class);
                    }
                }
        );

        DataStreamSource<String> dwd_inspect_detail = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_inspect_detail", groupId));
        SingleOutputStreamOperator<InspectDetail> inspectDetailStream = dwd_inspect_detail.map(
                new MapFunction<String, InspectDetail>() {
                    @Override
                    public InspectDetail map(String value) throws Exception {
                        return JSON.parseObject(value, InspectDetail.class);
                    }
                }
        );

        // TODO 2.指定WaterMark以及时间字段
        SingleOutputStreamOperator<InspectInfo> infoStreamWithWaterMark = inspectInfoStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<InspectInfo>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(InspectInfo element) {
                        return element.getTs();
                    }
                }
        );

        SingleOutputStreamOperator<InspectDetail> detailStreamWithWaterMark = inspectDetailStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<InspectDetail>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(InspectDetail element) {
                        return element.getTs();
                    }
                }
        );

        // TODO 3.双流Join
        KeyedStream<InspectInfo, String> keyByInspectIdInfoStream = infoStreamWithWaterMark.keyBy(
                new KeySelector<InspectInfo, String>() {
                    @Override
                    public String getKey(InspectInfo value) throws Exception {
                        return value.getOrderId();
                    }
                }
        );

        KeyedStream<InspectDetail, String> keyByInspectIdDetailStream = detailStreamWithWaterMark.keyBy(
                new KeySelector<InspectDetail, String>() {
                    @Override
                    public String getKey(InspectDetail value) throws Exception {
                        return value.getOrderId();
                    }
                }
        );

        SingleOutputStreamOperator<InspectWide> infoJoinDetailStream = keyByInspectIdInfoStream
                .intervalJoin(keyByInspectIdDetailStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(
                        new ProcessJoinFunction<InspectInfo, InspectDetail, InspectWide>() {
                            @Override
                            public void processElement(InspectInfo left, InspectDetail right, Context ctx, Collector<InspectWide> out) throws Exception {
                                out.collect(new InspectWide());
                            }
                        }
                );

        return infoJoinDetailStream;
    }
}
