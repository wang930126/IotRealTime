package controller.dimdwd;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.TableProcessFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.MyKafkaUtil;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * 此应用用于把数据库中的事实表数据导入kafka的dwd层中
 * 把维度表数据导入hbase的dim层中
 */
public class ods_db_2_dwd_dim {

    public static String groupId = ods_db_2_dwd_dim.class.getSimpleName();
    public static OutputTag<JSONObject> tag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(1000,TimeUnit.MILLISECONDS)));
        env.enableCheckpointing(60000,CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000"));
        env.setParallelism(10);
        String topic = "ods_db";

        // TODO 1.Maxwell从MySQL中拉取到kafka中的数据 过滤判断
        DataStreamSource<String> jsonStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjStream = jsonStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> filteredDStream = jsonObjStream.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        // table data字段不为空 且data字段长度大于3
                        boolean flag = jsonObject.getString("table") != null
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getString("data").length() > 3;

                        return flag;
                    }
                }
        );

        // TODO 2.使用一个ProcessFunction将dwd层的数据划到主流 将dim层的数据放到侧输出流中
        // TODO 这里的TableProcessFunction需要定时的从MySQL中获取动态分流的逻辑 将dwd的数据归到主流 将dim的数据归到侧输出流
        SingleOutputStreamOperator<JSONObject> dwdData = filteredDStream.process(new TableProcessFunction(tag));
        DataStream<JSONObject> sideData = dwdData.getSideOutput(tag);

        // TODO 3.维度数据保存到hbase
        sideData.addSink(new DimSink());

        // TODO 4.事实数据保存到kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaProducerBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject obj, @Nullable Long timestamp) {
                        String sinkTopic = obj.getString("sink_table");
                        JSONObject data = obj.getJSONObject("data");
                        return new ProducerRecord(
                                sinkTopic,
                                data.toString().getBytes()
                        );
                    }
                }
        );
        dwdData.addSink(kafkaSink);

        env.execute();

    }
}
