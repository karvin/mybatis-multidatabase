package com.karvin.shard;

import com.karvin.annotations.Catalog;
import com.karvin.annotations.Master;
import com.karvin.annotations.ShardBy;
import com.karvin.common.SelectorHolder;
import com.karvin.common.ShardKey;
import com.karvin.plugin.ReflectionUtils;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.reflection.ExceptionUtil;
import org.apache.ibatis.session.SqlSession;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by karvin on 16/8/18.
 */
public class ShardMapperProxy<T> implements InvocationHandler, Serializable {

    private static final long serialVersionUID = -6424540398559729838L;
    private final SqlSession sqlSession;
    private final Class<T> mapperInterface;
    private final Map<Method, MapperMethod> methodCache;

    public ShardMapperProxy(SqlSession sqlSession, Class<T> mapperInterface, Map<Method, MapperMethod> methodCache) {
        this.sqlSession = sqlSession;
        this.mapperInterface = mapperInterface;
        this.methodCache = methodCache;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Object.class.equals(method.getDeclaringClass())) {
            try {
                return method.invoke(this, args);
            } catch (Throwable t) {
                throw ExceptionUtil.unwrapThrowable(t);
            }
        }
        if(method.getDeclaringClass().equals(this.mapperInterface)){
            doBefore(method,args);
        }
        final MapperMethod mapperMethod = cachedMapperMethod(method);
        return mapperMethod.execute(sqlSession, args);
    }

    private MapperMethod cachedMapperMethod(Method method) {
        MapperMethod mapperMethod = methodCache.get(method);
        if (mapperMethod == null) {
            mapperMethod = new MapperMethod(mapperInterface, method, sqlSession.getConfiguration());
            methodCache.put(method, mapperMethod);
        }
        return mapperMethod;
    }

    private void doBefore(Method method,Object[] args){
        ShardKey key = this.buildShardKey(method, args);
        SelectorHolder.database.set(this.getCatalog(method));
        SelectorHolder.key.set(key);
    }

    private boolean useMaster(Method method){
        Master master = method.getAnnotation(Master.class);
        return master != null;
    }

    private String getCatalog(Method method){
        Catalog catalog = method.getAnnotation(Catalog.class);
        if(catalog != null){
            return catalog.value();
        }
        Class clazz = method.getDeclaringClass();
        catalog = (Catalog)clazz.getAnnotation(Catalog.class);
        if(catalog != null){
            return catalog.value();
        }
        return null;
    }

    private ShardKey buildShardKey(Method method,Object[] args){
        Annotation[][] annotations = method.getParameterAnnotations();
        if(annotations == null){
            return null;
        }
        ShardKey key = null;
        if(this.useMaster(method)){
            key = new ShardKey();
            key.setUseMaster(true);
        }
        for(int i=0;i<annotations.length;i++){
            Annotation[] annos = annotations[i];
            if(annos != null){
                for(int j=0;j<annos.length;j++){
                    Annotation annotation = annos[j];
                    if( annotation instanceof ShardBy){
                        if(key == null){
                            key = new ShardKey();
                        }
                        ShardBy shardBy = (ShardBy)annotation;
                        String field = shardBy.value();
                        Object obj = args[i];
                        if(!StringUtils.isEmpty(field)) {
                            obj = ReflectionUtils.getFieldValue(args[i], field);
                        }
                        key.setValue((Comparable)obj);
                        return key;
                    }
                }
            }
        }
        return key;
    }
}
