package controller.dimdwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.Constant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 将ods层中的各种log
 * 启动log（机床 机器人 测试间）关闭log 周期性(3min)心跳日志
 * 制造日志(机床 机器人 测试间 单位一次工艺的完成) main_log_
 * 制造明细日志(机床 机器人 测试间 粒度一步工序的完成)
 * 大型件完成进度日志(机床 机器人 测试间 质量检测)
 */
public class ods_log_2_dwd {


    public static void main(String[] args) throws Exception {

        // TODO 1.获取flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.设置并行度 一般设置成与上游Kafka的分区数一致
        env.setParallelism(4);

        // TODO 3.设置流处理的时间予以、Checkpoint语义、设置状态后端
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/user/flink/checkpoint/ods_log_2_dwd"));

        System.setProperty("HADOOP_USER_NAME", "wang930126");

        // TODO 4.获取Source
        // 这里需要传入一个FlinkKafkaConsumer对象 从工具类中生成出来
        String topic = Constant.ODS_LOG_TOPIC;
        String groupId = Constant.ODS_LOG_2_DWD_APP_GROUP_ID;
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaSource = env.addSource(consumer);

        // TODO 5.获取到的Source中是一个个String 需要解析其中数据 得转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.map(
                new MapFunction<String, JSONObject>() {
                    public JSONObject map(String log) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(log);
                        return jsonObject;
                    }
                }
        );

        // TODO 6.识别新老被加工件 每条加工日志中会有一个参数用于描述这个零件是否第一次被加工(返修)
        // 利用flink的状态管理机制 将所有零件的第一次加工的状态进行一个维护
        KeyedStream<JSONObject, String> partIdKeyedDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    public String getKey(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("common").getString("part_id");
                    }
                }
        );

        // 需要访问到状态就需要富函数或者ProcessFunction
        /**
         * json中有两个参数 is_new表示这个机器端认为的零件是不是新的(可能不准)
         * 另一个参数 ts表示当前日志时间戳
         */
        SingleOutputStreamOperator<JSONObject> pidWithNewFlagDS = partIdKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstProductDate;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> des = new ValueStateDescriptor<String>("firstProductDate", String.class);
                        firstProductDate = getRuntimeContext().getState(des);
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String is_new = jsonObject.getJSONObject("common").getString("is_new");
                        String log_ts = jsonObject.getJSONObject("common").getString("ts");
                        // 如果机器都认为不是新的了就可以直接放行 反之进入逻辑处理
                        // 如果是新的 我们需要看其状态是否存在 并且 状态日期小于等于这条日志
                        if ("1".equals(is_new)) {
                            String firstDate = firstProductDate.value();
                            // 如果状态存在 并且 状态日期小于日志日期 说明已经访问过 将is_new置0
                            if (firstDate != null && firstDate.length() != 0) {
                                if (!firstDate.equals(sdf.format(new Date(log_ts)))) {
                                    jsonObject.getJSONObject("common").put("is_new", 0);
                                }
                                // 如果状态不存在 说明确实是新的哦 更新key的状态即可
                            } else {
                                firstProductDate.update(sdf.format(new Date(log_ts)));
                            }
                        }
                        return jsonObject;
                    }
                }
        );

        final OutputTag<String> machine_tool_start_tag = new OutputTag<String>("MachineTool_Start");
        final OutputTag<String> weld_robot_start_tag = new OutputTag<String>("WeldRobot_Start");

        // TODO 7.将日志中的各种事件按照事件类型进行分流 利用侧输出流 利用侧输出流需要用到ProcessFunction
        // json字符串中有一个key 用于标记日志的类型
        SingleOutputStreamOperator<String> sideOutputDS = pidWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {



                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<String> out) throws Exception {
                        JSONObject startJsonObj = obj.getJSONObject("start");
                        String dataStr = obj.toString();
                        if (startJsonObj != null && startJsonObj.size() != 0) { //启动日志不为空说明可能是设备的启动类型的日志
                            String equipment_Type = startJsonObj.getString("type");
                            // 将启动日志输入到侧输出流中
                            switch (equipment_Type) {
                                case "MachineTool_Start":
                                    ctx.output(machine_tool_start_tag, dataStr);
                                    break;
                                case "WeldRobotStart":
                                    ctx.output(weld_robot_start_tag, dataStr);
                                    break;
                                default:
                            }
                        }

                        // 判断是否是关闭日志 以及返回给大屏的状态

                        // 判断是否是平时维护的心跳日志 此类日志可以用于统计设备此次的开机时长 直接推送到主流
                        out.collect(dataStr);

                        // 判断是否是制造日志

                        // 判断是否是大型件完成日志

                        // 判断是否是
                    }
                }
        );

        DataStream<String> machineSideOutput = sideOutputDS.getSideOutput(machine_tool_start_tag);
        String topic01 = "dwd_machine_start_log";
        FlinkKafkaProducer<String> kafkaProducer = MyKafkaUtil.getKafkaProducer(topic01);
        machineSideOutput.addSink(kafkaProducer);

        env.execute();
    }

}
