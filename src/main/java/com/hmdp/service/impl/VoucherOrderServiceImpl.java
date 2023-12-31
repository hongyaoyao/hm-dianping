package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;


    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 用于存放下单信息的阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct // 用于标记该方法在类初始化后立即执行
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明pending-list中没有异常消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                }
            }
        }
    }

    // 应该加上事务，但是不好加
    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }

    /**
     * 使用阻塞队列实现异步下单
    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            // 1.获取用户id
            Long userId = voucherOrder.getId();
            // 2.创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            // 3.尝试获取锁
            boolean isLock = lock.tryLock();
            // 4.判断是否获取锁成功
            if(!isLock){
                // 获取锁失败，直接返回失败或者重试
                log.error("不允许重复下单！");
                return;
            }
            try{
                proxy.createVoucherOrder(voucherOrder);
            }finally {
                // 释放锁
                lock.unlock();
            }
        }
    }

    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4.返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 最好不好在方法上加锁，这样锁住的是整个this对象
        // 而应该加锁的是用户（用户id），但是每个处理请求的线程中的userId都是一个全新的Long对象，所以可以转成字符串，并且使用intern方法将字符串加入常量池中，让所有字符串值相等的字符串是同一个对象
        // 查询是否已经购买过该秒杀券
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            // 用户已经购买过该秒杀券
            log.error("用户不能重复购买");
            return;
        }
        // 6.扣减库存，需要使用锁来保证线程安全，这里使用悲观锁中的CAS（因为是更新操作）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1") // set stock = stock - 1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) // where id = ? and stock > 0
                .update();
        if (!success) {
            // 扣减失败
            log.error("库存不足");
            return;
        }
        // 7.创建订单
        save(voucherOrder);
    }
    */

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 已经结束
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            // 库存不足
//            return Result.fail("库存不足");
//        }
//        /**
//         *  在集群或者分布式模式下使用Redisson实现的分布式锁来实现线程安全
//         */
//        Long userId = UserHolder.getUser().getId();
//        // 获取锁（可重入），指定锁的名称
//        RLock lock = redissonClient.getLock("order:" + userId);
//        // 尝试获取锁，参数分别是：获取锁的最大等待时间（期间会重试），锁自动释放时间，时间单位
//        boolean isLock = lock.tryLock();
//        // 判断锁是否获取成功
//        if (!isLock) {
//            // 获取锁失败
//            return Result.fail("用户不能重复购买");
//        }
//        try {
//            // 由于Spring中的事务是由动态代理实现的，即Spring生成了当前类的动态代理，再通过调用动态代理类中的事务方法来实现事务
//            // 而单纯的this.createVoucherOrder(voucherId)会使事务失效
//            // 获取和事务有关的代理对象，由代理对象来调用createVoucherOrder方法，这样事务才会生效
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
//        /**
//         *  在集群或者分布式模式下使用自定义的redis分布式锁来实现线程安全
//         Long userId = UserHolder.getUser().getId();
//         // 创建锁对象
//         SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//         // 尝试获取锁
//         boolean isLock = lock.tryLock(1200);
//         // 判断是否获取锁成功
//         if (!isLock) {
//         // 获取锁失败
//         return Result.fail("用户不能重复购买");
//         }
//         try {
//         // 由于Spring中的事务是由动态代理实现的，即Spring生成了当前类的动态代理，再通过调用动态代理类中的事务方法来实现事务
//         // 而单纯的this.createVoucherOrder(voucherId)会使事务失效
//         // 获取和事务有关的代理对象，由代理对象来调用createVoucherOrder方法，这样事务才会生效
//         IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//         return proxy.createVoucherOrder(voucherId);
//         } finally {
//         // 释放锁
//         lock.unlock();
//         }
//         */
//        /**
//         *  单机模式下使用synchronized来实现线程安全
//         Long userId = UserHolder.getUser().getId();
//         // 5.一人一单，不能重复购买，必须使用锁保证线程安全，这里使用悲观锁（因为只是查询操作，使用不了CAS）
//         // 锁的释放必须要在事务提交后才能释放，即createVoucherOrder方法执行完才释放，这样是防止锁在createVoucherOrder方法内释放，而此时事务还没有提交，数据还没有写入，再有线程查询会出现线程安全问题
//         synchronized (userId.toString().intern()) {
//         // 由于Spring中的事务是由动态代理实现的，即Spring生成了当前类的动态代理，再通过调用动态代理类中的事务方法来实现事务
//         // 而单纯的this.createVoucherOrder(voucherId)会使事务失效
//         // 获取和事务有关的代理对象，由代理对象来调用createVoucherOrder方法，这样事务才会生效
//         IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//         return proxy.createVoucherOrder(voucherId);
//         }
//         */
//    }
//
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 最好不好在方法上加锁，这样锁住的是整个this对象
//        // 而应该加锁的是用户（用户id），但是每个处理请求的线程中的userId都是一个全新的Long对象，所以可以转成字符串，并且使用intern方法将字符串加入常量池中，让所有字符串值相等的字符串是同一个对象
//        // 查询是否已经购买过该秒杀券
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if (count > 0) {
//            // 用户已经购买过该秒杀券
//            return Result.fail("用户不能重复购买");
//        }
//        // 6.扣减库存，需要使用锁来保证线程安全，这里使用悲观锁中的CAS（因为是更新操作）
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1") // set stock = stock - 1
//                .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
//                .update();
//        if (!success) {
//            // 扣减失败
//            return Result.fail("库存不足");
//        }
//        // 7.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 7.1 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 7.2 用户id
//        voucherOrder.setUserId(userId);
//        // 7.3 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//        // 8.返回订单id
//        return Result.ok(orderId);
//    }
}
