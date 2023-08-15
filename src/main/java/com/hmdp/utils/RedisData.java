package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

@Data
/**
 * 将实体类封装逻辑过期时间
 */
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
