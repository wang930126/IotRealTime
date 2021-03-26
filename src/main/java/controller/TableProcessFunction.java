package controller;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.MyMySQLUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 *
 */
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    // 侧输出流(hbase)的tag
    private OutputTag<JSONObject> tag;
    // 用于存储 MySQL表名 -> 动态分流逻辑
    private Map<String, TableProcess> map = new HashMap<String, TableProcess>();
    // 因为维度数据要进入hbase中，因此需要Phoenix连接
    private Connection conn;
    private Set<String> existingTables = new HashSet<String>();

    public TableProcessFunction(OutputTag<JSONObject> tag) {
        this.tag = tag;
    }

    /**
     * 用于初始化一些连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection("");

        // TODO 初始化map
        initMap();
        // TODO 设置定时器 定时的去调度更新map的任务
        Timer timer = new Timer();
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        initMap();
                    }
                },5000,5000
        );
    }

    // TODO 读取MySQL中的信息 存入Map中 对于insert进Hbase的表需要检查HBase中是否有已经建好的表
    private void initMap() {
        List<TableProcess> list = MyMySQLUtil.queryList("select * from table_process",
                TableProcess.class, true);

        for (TableProcess process : list) {
            String sourceTable = process.getSourceTable();
            String operateType = process.getOperateType();
            String pkColumn = process.getPkColumn();
            String sinkColumns = process.getSinkColumns();
            String sinkTable = process.getSinkTable();
            String sinkType = process.getSinkType();
            String sinkExtend = process.getSinkExtend();
            String key = sourceTable + ":" + operateType; //"user:insert"
            map.put(key,process);

            // 如果是向hbase中怼数据 需要判断hbase中是否有表了
            if("insert".equals(operateType) && "hbase".equals(sinkType)){
                boolean flag = existingTables.add(sourceTable);
                if(flag){
                    checkTable(sinkTable,sinkColumns,pkColumn,sinkExtend);
                }
            }
        }
    }

    // 此方法用于通过Phoenix在Hbase中创建表
//    create table if not exists db_name.table_name(
//          id varchar primary key,
//    info.col_name varchar,
//    info.col_name1 varchar
//    )
    private void checkTable(String sinkTable,
                            String sinkColumns,
                            String pkColumn,
                            String sinkExtend) {
        if(pkColumn == null || "".equals(pkColumn)){
            pkColumn = "id";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ");
        sb.append("db_name.");
        sb.append(sinkTable);
        sb.append("(");
        String[] cols = sinkColumns.split(",");
        for(int i=0;i<cols.length;i++){
            if(pkColumn.equals(cols[i])){
                sb.append(cols[i] + " varchar primary key");
            }else{
                sb.append("info." + cols[i] + " varchar");
            }
            if(i < cols.length - 1){
                sb.append(",");
            }
        }
        sb.append(")");
        System.out.println(sb.toString());
        PreparedStatement pst = null;

        try{
            pst = conn.prepareStatement(sb.toString());
            pst.execute();
        }catch(Exception e){

        }finally {
            if(pst != null){
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 处理逻辑如下：对每条json数据(从maxwell中过来的) 构建主键 table:type 从map中得到process对象
     * 进而得到sinktable 以及 sink到的数据库(kafka hbase)
     * @param json
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject json, Context ctx, Collector<JSONObject> out) throws Exception {
        String tbName = json.getString("table");
        String type = json.getString("type");
        String key = tbName + ":" + type;
        TableProcess process = map.get(key);
        if(process != null){
            json.put("sink_table",process.getSinkTable());
        }else{
            System.out.println("key not exists!");
        }
        // TODO 分流
        if(process != null && "kafka".equalsIgnoreCase(process.getSinkType())){
            out.collect(json);
        }else if(process != null && "hbase".equalsIgnoreCase(process.getSinkType())){
            ctx.output(tag,json);
        }
    }

    public static void main(String[] args) {

    }

}
