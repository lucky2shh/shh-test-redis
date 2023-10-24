package cn.shh.test.redis.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class JedisPoolFactory {
    private static final JedisPool jedisPoll;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(4);
        jedisPoolConfig.setMinIdle(2);
        jedisPoolConfig.setMaxWait(Duration.ofSeconds(3));
        jedisPoll = new JedisPool(jedisPoolConfig,
                "127.0.0.1",
                6379,
                3000,
                "redis368.cn");
    }

    public static Jedis getJedisPoll(){
        return jedisPoll.getResource();
    }
}
