package cn.shh.test.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class JedisTest {
    private Jedis jedis;

    @Test
    public void testString(){
        String setResult = jedis.set("k2", "v2");
        System.out.println("setResult = " + setResult); // OK

        String getResult = jedis.get("k2");
        System.out.println("getResult = " + getResult); // v2
    }

    @BeforeEach
    public void prepareJedis(){
        jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("redis368.cn");
        jedis.select(0);
    }

    @AfterEach
    public void closeJedis(){
        if (jedis != null) {
            jedis.close();
        }
    }
}