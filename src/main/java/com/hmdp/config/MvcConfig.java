package com.hmdp.config;

import com.hmdp.interceptor.LoginInterceptor;
import com.hmdp.interceptor.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 * @PROJECT_NAME: hm-dianping
 * @PACKAGE_NAME: com.hmdp.config
 * @CLASS_NAME: MvcConfig
 * @USER: hongyaoyao
 * @DATETIME: 2023/8/9 21:09
 * @Emial: 1299694047@qq.com
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    // 拦截器的order值越低，执行优先级越高
    public void addInterceptors(InterceptorRegistry registry) {
        // 登录拦截器，拦截需要用户校验的请求，查看是否已经登录
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        "blog/hot",
                        "/shop/**",
                        "/voucher/**",
                        "/shop-type/**",
                        "/user/code",
                        "/user/login"
                ).order(1);
        // token刷新的拦截器，解决访问不需要用户校验的页面也会刷新token（假设登录后一直访问不需要用户校验的页面，那么token的有效期不会刷新，但是这是不合理的）
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                .addPathPatterns("/**").order(0);
    }
}
