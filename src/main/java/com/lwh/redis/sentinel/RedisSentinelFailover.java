package com.lwh.redis.sentinel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisSentinelFailover {

    private static Logger logger = LoggerFactory.getLogger(RedisSentinelFailover.class);

    public static void main(String[] args) {

        String masterName = "mymaster";

        Set<String> sentinels = new HashSet<>();
        sentinels.add("192.168.25.3:26379");
        sentinels.add("192.168.25.3:26380");
        sentinels.add("192.168.25.3:26381");

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(masterName, sentinels);

        int count = 0;

        while(true){
            count++;
            Jedis jedis = null;
            try {
                jedis = jedisSentinelPool.getResource();

                int index = new Random().nextInt(10000);
                String key = "k-" + index;
                String value = "v-" + index;
                jedis.set(key, value);

                if(count % 100 == 0){
                    System.out.println(key + ", " + jedis.get(key));
                }

                TimeUnit.MILLISECONDS.sleep(10);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                if(jedis != null){
                    jedis.close();
                }
            }
        }

    }
}
