package com.lwh.redis.lock;

import redis.clients.jedis.Jedis;

import java.util.Collections;

/**
 * 为了保证分布式锁可用,需要满足的条件
 * 1.互斥性,在任意时刻,只有一个客户端能持有锁
 * 2.不会发生死锁,即使有一个客户端在持有锁期间崩溃没有主动解锁,也能保证后序客户端能加锁
 * 3.解铃还需系铃人,加锁解锁必须是同一个客户端,客户端自己不能把别人加的锁给解了
 * 4.具有容错性
 */
public class SimpleLock {


    private static final String LOCK_SUCCESS = "OK";

    private static final String SET_IF_NOT_EXIST = "NX";

    private static final String SET_WITH_EXPIRE_TIME = "PX";

    private static final Long RELEASE_SUCCESS = 1L;

    /**
     * 尝试获取分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁,set操作加入了NX参数,可以保证如果key存在,则函数不会调用成功,也就是只有一个客户端能持有锁,满足条件1互斥性
     * @param requestId 请求标识,要解锁时,必须提供一个requestId,与此处设置的requestId相比,鉴定加锁解锁的是否是同一个客户端,满足条件3
     * @param expireTime 超时时间,对锁设置了超时时间,即使锁的持有者后续发生崩溃没有解锁,锁也会因为到了过期时间而自动解锁,不会发生死锁,满足条件2
     * @return 是否获取锁成功
     */
    public static boolean tryGetDistributedLock(Jedis jedis, String lockKey, String requestId, int expireTime){

        /** 原子操作 **/
        String res = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);

        if(LOCK_SUCCESS.equals(res)){
            return true;
        }

        return false;
    }

    /**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 用来鉴定是否加锁解锁是同一客户端
     * @return 是否释放锁成功
     */
    public static boolean releaseDistributedLock(Jedis jedis, String lockKey, String requestId){
        /** 简单的Lua脚本,用来获取锁对应的value值,检测是否与requestId相等,如果相等则删除锁 **/
        /** 为什么使用Lua语言,为了保证命令是原子性的,eval方法执行该命令可以保证原子性 **/
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object res = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

        if(RELEASE_SUCCESS.equals(res)){
            return true;
        }

        return false;
    }

    /** 错误写法 **/
    public static void wrongGetLock1(Jedis jedis, String lockKey, String requestId, int expireTime) {
        /** 关键原因是setnx和expire是两条命令,不具有原子性**/
        Long result = jedis.setnx(lockKey, requestId);

        if (result == 1) {
            /** 若在这里程序突然崩溃,则无法设置过期时间,将发生死锁 **/
            jedis.expire(lockKey, expireTime);
        }
    }




}
