package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1.从redis查询商铺类型信息
        String key = CACHE_SHOP_TYPE_KEY;
        List<String> shopTypeJsonList = stringRedisTemplate.opsForList().range(key, 0, -1);
        // 2.判断redis中是否存在商铺类型信息
        if (!shopTypeJsonList.isEmpty()) {
            // 3.redis中存在，直接返回
            List<ShopType> shopTypeList = new ArrayList<>();
            for (String jsonStr : shopTypeJsonList) {
                shopTypeList.add(JSONUtil.toBean(jsonStr, ShopType.class));
            }
            return Result.ok(shopTypeList);
        }
        // 4.redis中不存在，查询数据库中所有商铺类型
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        // 5.数据库不存在，返回错误
        if (shopTypeList.isEmpty()) {
            return Result.fail("店铺不存在");
        }
        // 6.数据库存在，写入redis中
        for (ShopType shopType : shopTypeList) {
            shopTypeJsonList.add(JSONUtil.toJsonStr(shopType));
        }
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypeJsonList);
        stringRedisTemplate.expire(key, CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);
        // 7.返回商铺信息
        return Result.ok(shopTypeList);
    }
}
