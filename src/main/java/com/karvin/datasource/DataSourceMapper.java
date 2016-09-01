package com.karvin.datasource;

import com.karvin.common.SelectorHolder;
import com.karvin.common.ShardKey;
import com.karvin.rule.RuleInstance;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Created by karvin on 16/8/16.
 */
public class DataSourceMapper implements DataSource {

    public Map<String, DataSource> getDataSourceMap() {
        return dataSourceMap;
    }

    public void setDataSourceMap(Map<String, DataSource> dataSourceMap) {
        this.dataSourceMap = dataSourceMap;
    }

    private Map<String,DataSource> dataSourceMap = new ConcurrentHashMap<String,DataSource>();

    private DataSource getDataSource(){
        ShardKey key = SelectorHolder.key.get();
        String sourceName = SelectorHolder.database.get();
        DataSource dataSource = dataSourceMap.get(sourceName);
        if(dataSource == null){
            throw new IllegalArgumentException("no datasource named:"+sourceName);
        }
        dataSource = this.selectDataSource(dataSource);
        if(key.isUseMaster()&&isMasterSlaveDataSource(dataSource)){
            MasterSlaveDataSource masterSlaveDataSource = (MasterSlaveDataSource)dataSource;
            return masterSlaveDataSource.getMasterDataSource();
        }
        return dataSource;
    }

    private DataSource selectDataSource(DataSource dataSource){
        if(dataSource instanceof ListableDataSource){
            ShardKey key = SelectorHolder.key.get();
            int index = RuleInstance.getInstance().match().decideDataSourceIndex(key);
            ListableDataSource listableDataSource = (ListableDataSource)dataSource;
            return listableDataSource.getByIndex(index);
        }
        return dataSource;
    }

    private boolean isMasterSlaveDataSource(DataSource dataSource){
        if(dataSource == null){
            return false;
        }
        if(dataSource instanceof MasterSlaveDataSource){
            return true;
        }
        return false;
    }

    public Connection getConnection() throws SQLException {
        return this.getDataSource().getConnection();
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return this.getDataSource().getConnection(username, password);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return this.getDataSource().getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.getDataSource().setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        this.getDataSource().setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return this.getDataSource().getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.getDataSource().getParentLogger();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.getDataSource().unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getDataSource().isWrapperFor(iface);
    }
}
