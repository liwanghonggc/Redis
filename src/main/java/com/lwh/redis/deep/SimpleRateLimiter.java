package com.lwh.redis.deep;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;

/**
 * @author lwh
 * @date 2019-03-16
 * @desp 简单限流策略
 */
public class SimpleRateLimiter {

    private final Jedis jedis;

    public SimpleRateLimiter(Jedis jedis){
        this.jedis = jedis;
    }

    public boolean isActionAllowed(String userId, String actionKey, int period, int maxCount) throws IOException {
        String key = String.format("hist:%s:%s", userId, actionKey);
        long nowTs = System.currentTimeMillis();

        Pipeline pipeline = jedis.pipelined();
        //开启一个事务
        pipeline.multi();
        //value和score都用毫秒时间戳
        pipeline.zadd(key, nowTs, "" + nowTs);
        //移除时间窗口之外的行为记录,剩下的都是时间窗口内的
        pipeline.zremrangeByScore(key, 0, nowTs - period * 1000);
        //获得[nowTs - period * 1000, nowTs]的key的数量
        Response<Long> count = pipeline.zcard(key);
        //每次设置都更新key的过期时间
        pipeline.expire(key, period);

        //在事务中执行上述命令
        pipeline.exec();
        pipeline.close();

        return count.get() <= maxCount;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Jedis jedis=new Jedis("localhost",6379);
        SimpleRateLimiter limiter=new SimpleRateLimiter(jedis);
        for (int i = 0; i < 20; i++) {
            //每个用户在1秒内最多能做五次动作
            System.out.println(limiter.isActionAllowed("lwh","reply",1,5));
        }
    }
}
