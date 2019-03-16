package com.lwh.redis.deep;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * @author lwh
 * @date 2019-03-16
 * @desp Redis实现延时队列
 */
public class RedisDelayingQueue<T> {

    static class TaskItem<T> {
        public String id;
        public T msg;
    }

    private Type TaskType = new TypeReference<TaskItem<T>>(){}.getType();

    private Jedis jedis;

    private String queueKey;

    public RedisDelayingQueue(Jedis jedis, String queueKey){
        this.jedis = jedis;
        this.queueKey = queueKey;
    }

    public void delay(T msg){
        TaskItem taskItem = new TaskItem();
        taskItem.id = UUID.randomUUID().toString();
        taskItem.msg = msg;
        String s = JSON.toJSONString(taskItem);
        jedis.zadd(queueKey, System.currentTimeMillis() + 5000, s);
    }

    public void loop(){
        while (!Thread.interrupted()){
            //只取一条数据
            Set values = jedis.zrangeByScore(queueKey, 0, System.currentTimeMillis(), 0, 1);
            if(values.isEmpty()){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }

            String s = (String) values.iterator().next();
            //zrem用于移除有序集中一个或多个成员
            if(jedis.zrem(queueKey, s) > 0){
                TaskItem task = JSON.parseObject(s, TaskType);
                this.handleMsg(task.msg);
            }
        }
    }

    private void handleMsg(Object msg) {
        System.out.println(msg);
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis();
        RedisDelayingQueue queue = new RedisDelayingQueue(jedis, "test-queue");

        Thread producer = new Thread(){
            @Override
            public void run() {
                for (int i = 0; i < 10; i++){
                    queue.delay("lwh" + i);
                }
            }
        };

        Thread consumer = new Thread(){
            @Override
            public void run() {
                queue.loop();
            }
        };

        producer.start();
        consumer.start();

        try {
            producer.join();
            Thread.sleep(6000);
            consumer.interrupt();
            consumer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
