package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;


import java.util.List;

public class DimInfoUtil {
    /**
     * 缓存未命中 调用Phoenix API去HBase中查询数据
     *
     * @return 从hbase中查找到的一条数据
     */
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... name2Value) {
        String sql = "select * from " + tableName + " where " + "id = '1' and age = '3'";
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject res = null;
        if(jsonObjects != null && jsonObjects.size() > 0){
            res = jsonObjects.get(0);
        }else{
            System.out.println("failed...");
        }
        return res;
    }

    public static JSONObject getDimInfo(String tableName,Tuple2<String,String>... colName2value){
        return null;
    }

    public static JSONObject getDimInfo(String tableName,String keyId){
        return getDimInfo(tableName,Tuple2.of("id",keyId));
    }

}
