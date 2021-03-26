package controller.dwm;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import util.DimInfoUtil;
import util.ThreadPoolUtil;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    private ExecutorService pool;
    private String tableName;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if(pool == null){pool = ThreadPoolUtil.getPool(); }
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                // TODO 1.来一条InspectWide消息 需要获取要关联的表的主键id(待实现)
                String keyId = getKeyId(input);
                // TODO 2.通过主键id 表名 获得redis的key 向redis查缓存 命中或者不命中 最后会得到一个代表维度数据的JSONObject
                JSONObject obj = DimInfoUtil.getDimInfo(tableName, Tuple2.of("id", keyId));
                // TODO 3.获得维度数据后 将InspectWide与维度数据join(待实现)
                join(input,obj);
                // TODO 4.返回ResultFuture对象
                resultFuture.complete(Arrays.asList(input));
            }
        });
    }
}
