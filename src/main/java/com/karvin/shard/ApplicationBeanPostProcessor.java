package com.karvin.shard;

import com.karvin.datasource.DataSourceFactory;
import com.karvin.plugin.ReflectionUtils;
import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by karvin on 16/8/17.
 */
@Service
public class ApplicationBeanPostProcessor implements ApplicationContextAware,BeanDefinitionRegistryPostProcessor{

    private MapperRegistry registry;

    private ApplicationContext applicationContext;

    private BeanNameGenerator generator = new DefaultBeanNameGenerator();

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        GenericBeanDefinition definition = new GenericBeanDefinition();
        definition.setBeanClassName(DataSourceFactory.class.getName());
        definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_BY_TYPE);
        String beanName = this.generator.generateBeanName(definition,registry);
        registry.registerBeanDefinition(beanName,definition);
        String[] factoryNames = applicationContext.getBeanNamesForType(SqlSessionFactory.class);
        if(factoryNames !=null && factoryNames.length>0){
            for(String factoryBean:factoryNames){
                BeanDefinition beanDefinition = registry.getBeanDefinition(factoryBean);
                beanDefinition.getPropertyValues().addPropertyValue("dataSource",new RuntimeBeanReference(beanName));
            }
        }

    }

    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        SqlSessionFactory sqlSessionFactory = beanFactory.getBean(SqlSessionFactory.class);
        MapperRegistry registry = sqlSessionFactory.getConfiguration().getMapperRegistry();
        Collection<Class<?>> mappers = registry.getMappers();
        Map<Class<?>,ShardMapperProxyFactory> map = new HashMap<Class<?>, ShardMapperProxyFactory>();
        for(Class<?> clazz:mappers){
            map.put(clazz,new ShardMapperProxyFactory(clazz));
        }
        ReflectionUtils.setFieldValue(registry, "knownMappers", map);
    }
}
