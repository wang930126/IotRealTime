package util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool pool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5); //连接池中最多5个空闲
        config.setMinIdle(5); //连接池中最少5个空闲
        config.setTestOnBorrow(true); //
        config.setMaxTotal(100); //连接池中最多100个连接 多了就block
        config.setBlockWhenExhausted(true); //
        config.setMaxWaitMillis(2000); //最多block2000ms 多了就exception
        pool = new JedisPool(config, "hadoop102", 6379, 1000);
    }

    public static Jedis getJedisConn() {
        return pool.getResource();
    }
}
