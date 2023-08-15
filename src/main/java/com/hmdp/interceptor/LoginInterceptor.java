package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * @PROJECT_NAME: hm-dianping
 * @PACKAGE_NAME: com.hmdp.interceptor
 * @CLASS_NAME: LoginInterceptor
 * @USER: hongyaoyao
 * @DATETIME: 2023/8/9 20:40
 * @Emial: 1299694047@qq.com
 */
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        // 判断是否需要拦截（ThreadLocal中是否有用户）
        if (UserHolder.getUser() == null) {
            // 没有用户，拦截，返回401（未授权）状态码
            response.setStatus(401);
            return false;
        }
        // 有用户，则放行
        return true;
    }
}
