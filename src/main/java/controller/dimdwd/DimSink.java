package controller.dimdwd;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import util.RedisUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class DimSink extends RichSinkFunction<JSONObject> {

    Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection("");
    }

    /**
     * 生成语句提交给hbase
     * upsert into db_name.table_name(colName1,colName2,colName3) values('123','423','333');
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        String tableName = value.getString("sink_table");
        JSONObject data = value.getJSONObject("data"); // "key1":"value1" , "key2":"value2"
        String sql = "upsert into db_name." + tableName + "(" + StringUtils.join(data.keySet(), ',')
                + ")" + "values('" + StringUtils.join(data.values(), "','") + "')";

        try {
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.executeUpdate();
            conn.commit(); //Phoenix默认是不自动提交事务的
            pst.close();
        } catch (Exception e) {

        } finally {

        }

        if (value.getString("operateType").equals("update")) {
            Jedis jedis = null;
            try {
                jedis = RedisUtil.getJedisConn();
                jedis.del("dim:" + tableName + ":" + value.getString("id"));
            }finally {
                if(jedis != null){
                    jedis.close();
                }
            }

        }
    }
}
