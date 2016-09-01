package com.karvin.plugin;

import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;


@Intercepts({@Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class })})
public class ShardInterceptor implements Interceptor {

    private Logger logger = LoggerFactory.getLogger(ShardInterceptor.class);

    public Object intercept(Invocation invocation) throws Throwable {
        StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
        MappedStatement mappedStatement = null;
        if (statementHandler instanceof RoutingStatementHandler) {
            StatementHandler delegate = (StatementHandler) ReflectionUtils
                    .getFieldValue(statementHandler, "delegate");
            mappedStatement = (MappedStatement) ReflectionUtils.getFieldValue(
                    delegate, "mappedStatement");
        } else {
            mappedStatement = (MappedStatement) ReflectionUtils.getFieldValue(
                    statementHandler, "mappedStatement");
        }
        String mapperId = mappedStatement.getId();
        String sql = statementHandler.getBoundSql().getSql();
        Object params = statementHandler.getBoundSql().getParameterObject();
        logger.info("before shard sql {}", sql);
        sql = SqlConverter.getInstance().convertSQL(sql, params, mapperId);
        ReflectionUtils.setFieldValue(statementHandler.getBoundSql(), "sql", sql);
        logger.info("after shard sql {}", statementHandler.getBoundSql().getSql());
        return invocation.proceed();
    }

    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    public void setProperties(Properties properties) {

    }

}
