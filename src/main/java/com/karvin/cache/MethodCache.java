package com.karvin.cache;

import com.karvin.common.ShardKey;
import com.karvin.datasource.ListableDataSource;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karvin on 16/8/16.
 */
@Service
public class MethodCache {

    private Map<String,Map<String,Method>> caches = new ConcurrentHashMap<String,Map<String,Method>>();

    public void scan(List<Class> classes){
        if(classes == null || classes.size() == 0)
            return;
        for(Class clazz:classes){
            this.scan(clazz);
        }
    }

    public void scan(Class clazz){
        if(clazz.equals(Object.class)){
            return;
        }
        String className = clazz.getName();
        Map<String,Method> methodCache = caches.get(className);
        if(methodCache != null){
            return;
        }
        Method[] methods = clazz.getDeclaredMethods();
        if(methods == null || methods.length == 0){
            return;
        }
        methodCache = new HashMap<String, Method>();
        for(Method method: methods){
            String name = method.getName();
            methodCache.put(name,method);
        }
        caches.put(className, methodCache);
        Class superClass = clazz.getSuperclass();
        Class[] interfaces = clazz.getInterfaces();
        List<Class> parent = new ArrayList<Class>();
        for(Class iface:interfaces){
            parent.add(iface);
        }
        if(superClass != null) {
            parent.add(superClass);
        }
        scan(parent);
    }

    public Method getMethod(Class clazz,String methodName){
        if(clazz == null){
            return null;
        }
        Map<String,Method> methodCache = caches.get(clazz.getName());
        if(methodCache != null){
            Method method = methodCache.get(methodName);
            if(method != null){
                return method;
            }
        }
        List<Class> parents = new ArrayList<Class>();
        Class superClass = clazz.getSuperclass();
        Class[] interfaces = clazz.getInterfaces();
        if(superClass != null){
            parents.add(superClass);
        }
        for(Class iface : interfaces){
            parents.add(iface);
        }
        return this.getMethod(parents,methodName);
    }

    private Method getMethod(List<Class> classes,String methodName){
        if(classes == null || classes.size() == 0){
            return null;
        }
        for(Class clazz:classes){
            Method method = this.getMethod(clazz,methodName);
            if(method != null){
                return method;
            }
        }
        return null;
    }

    public static void main(String[] args){
        MethodCache methodCache = new MethodCache();
        List<Class> classes = new ArrayList<Class>();
        classes.add(ListableDataSource.class);
        classes.add(ShardKey.class);
        methodCache.scan(classes);
        Method method = methodCache.getMethod(ShardKey.class,"isUseMaster");
    }

}
