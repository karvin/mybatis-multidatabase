package com.karvin.datasource;

import com.karvin.common.SelectorHolder;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Created by karvin on 16/8/16.
 */
public class MasterSlaveDataSource implements DataSource{

    private DataSource master;
    private ListableDataSource slaves;

    public MasterSlaveDataSource(DataSource master,ListableDataSource slaves){
        this.master = master;
        if(slaves != null) {
            this.slaves = slaves;
        }else{
            this.slaves = ListableDataSource.EMPTY_LISTABLE_DATASOURCE;
        }
    }

    public DataSource getMasterDataSource(){
        return this.master;
    }

    public DataSource getSlaveDataSource(){
        return this.slaves;
    }

    public Connection getMasterConnection() throws SQLException {
        return master.getConnection();
    }

    public Connection getMasterConnection(String username,String password) throws SQLException {
        return master.getConnection(username, password);
    }

    public Connection getSlaveConnection() throws SQLException {
        if(slaves.isEmpty() ){
            return this.getMasterConnection();
        }
        return this.slaves.getConnection();
    }

    public Connection getSlaveConnection(String username,String password) throws SQLException {
        if(slaves.isEmpty()){
            return this.getMasterConnection(username,password);
        }
        return slaves.getConnection(username, password);
    }

    public Connection getConnection() throws SQLException {
        if(SelectorHolder.key.get().isUseMaster()) {
            return this.getMasterConnection();
        }
        return this.getSlaveConnection();
    }

    public Connection getConnection(String username, String password) throws SQLException {
        if(SelectorHolder.key.get().isUseMaster()) {
            return this.getMasterConnection(username, password);
        }
        return this.getSlaveConnection(username,password);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return this.master.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.master.setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        this.master.setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return this.master.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.master.getParentLogger();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.master.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.master.isWrapperFor(iface);
    }
}
