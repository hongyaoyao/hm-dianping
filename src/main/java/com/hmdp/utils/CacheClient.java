package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_NULL_UNIT;

/**
 * @PROJECT_NAME: hm-dianping
 * @PACKAGE_NAME: com.hmdp.utils
 * @CLASS_NAME: CacheClient
 * @USER: hongyaoyao
 * @DATETIME: 2023/8/12 17:06
 * @Emial: 1299694047@qq.com
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 向redis中插入String类型的数据，并设置实际的过期时间
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 向向redis中插入String类型的数据，不设置过期时间，而是设置逻辑上的过期时间（在对象中封装一个过期时间，但是在redis不设置过期时间）
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        if (unit == TimeUnit.MINUTES) {
            redisData.setExpireTime(LocalDateTime.now().plusMinutes(unit.toMinutes(time)));
        }else if (unit == TimeUnit.SECONDS){
            redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        }else{
            redisData.setExpireTime(LocalDateTime.now().plusHours(unit.toHours(time)));
        }
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 使用“缓存空值”解决缓存穿透问题的代码的封装
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 4.判断命中的是否是空值
        if (json != null) {
            // 查询到缓存的空值，说明发生缓存穿透，直接返回，而不用查询数据库，减轻数据库压力
            return null;
        }

        // 5.缓存中不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 6.数据库中不存在
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, CACHE_NULL_UNIT);
            // 直接返回
            return null;
        }
        // 7.数据库中存在，写入redis
        this.set(key, r, time, unit);
        // 8.返回
        return r;
    }

    /**
     * 缓存击穿：高并发访问、缓存重建时间长的热点数据过期
     * 使用“逻辑过期”解决缓存击穿问题的代码的封装
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, String lockKeyPrefix,ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断缓存数据是否存在
        if (StrUtil.isBlank(json)) {
            // 3.不存在，直接返回，一般都会提前将热点数据加入缓存
            return null;
        }
        // 4.redis中存在数据，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 6.未过期，直接返回店铺信息
            return r;
        }
        // 7.已过期，需要缓存重建
        // 7.1.获取互斥锁
        String lockKey = lockKeyPrefix + id;
        boolean isLock = tryLock(lockKey);
        // 7.2.判断是否获取锁成功
        if (isLock){
            // 7.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R newR = dbFallback.apply(id);
                    // 重建缓存
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 7.4.释放锁
                    unlock(lockKey);
                }
            });
        }
        // 8.不管是否获取锁成功，最后都要返回过期的商铺信息
        return r;
    }

    /**
     * 缓存击穿：高并发访问、缓存重建时间长的热点数据过期
     * 使用“互斥锁”解决缓存击穿问题的代码的封装（在queryWithPassThrough的代码基础上增加使用“互斥锁”解决缓存击穿问题的代码）
     */
    public <R, ID> R queryWithMutex(String keyPrefix, String lockKeyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断缓存中是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        // 4.判断命中的是否是空值
        if (shopJson != null) {
            // 是空值则直接返回
            return null;
        }
        // 5.实现缓存重建
        // 5.1.获取互斥锁
        String lockKey = lockKeyPrefix + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 5.2.判断是否获取成功
            if (!isLock) {
                // 5.3.获取锁失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, lockKeyPrefix, id, type, dbFallback, time, unit);
            }
            // 5.4.获取锁成功，根据id查询数据库
            r = dbFallback.apply(id);
            // 6.如果数据库中不存在数据
            if (r == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 直接返回
                return null;
            }
            // 7.存在，将数据写入redis
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // 8.释放锁
            unlock(lockKey);
        }
        // 9.返回
        return r;
    }

    /**
     * 用于缓存击穿时缓存重建时的互斥锁，使用redis中的setnx命令实现
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "lock", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放互斥锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
