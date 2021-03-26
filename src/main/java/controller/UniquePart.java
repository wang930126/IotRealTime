package controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 用于实时计算当天制造的零件的总种类
 */
public class UniquePart {

    public static String topic = "dwd_manufacture_log";
    public static String groupId = UniquePart.class.getSimpleName();

    public static void main(String[] args) throws Exception {

        // TODO 1.从DWD层获取Source
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kfSource = env.addSource(kafkaConsumer);

        // TODO 2.转换结构 转成jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObject = kfSource.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );

        // TODO 3.按照partId keyBy 利用keyedState判定是否是每日的第一个该种类的零件
        KeyedStream<JSONObject, String> keyByPartIdStream = jsonObject.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        String partId = value.getJSONObject("common").getString("partId");
                        return partId;
                    }
                }
        );

        SingleOutputStreamOperator<JSONObject> uid = keyByPartIdStream.filter(
                new RichFilterFunction<JSONObject>() {

                    private SimpleDateFormat sdf = null;
                    private ValueState<String> lastMDate = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        ValueStateDescriptor<String> des = new ValueStateDescriptor<>("lastMDate", String.class);
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.days(1))
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                // 设置状态的可见性 状态超过TTL之后就不会反回了
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                // ？？？
                                .build();

                        des.enableTimeToLive(ttlConfig);
                        lastMDate = getRuntimeContext().getState(des);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        // todo 获取制造的时间戳
                        Long ts = jsonObject.getLong("ts");
                        String date = sdf.format(new Date(ts));
                        String lastDate = lastMDate.value();

                        // 如果日志中的数据解析出来跟上次状态一样 那么就不是首次 直接过滤
                        if (date != null && date.length() > 0 && date.equals(lastDate)) {
                            return false;
                        } else {
                            lastMDate.update(date);
                            return true;
                        }
                    }
                }
        ).uid("unique_part_filter");

        SingleOutputStreamOperator<String> upJsonStringStream = uid.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toJSONString();
                    }
                }
        );

        upJsonStringStream.print("...");

        // todo 4.过滤后的UniquePart写入DWM层
        upJsonStringStream.addSink(MyKafkaUtil.getKafkaProducer("dwm_unique_part"));

        env.execute();

    }
}
