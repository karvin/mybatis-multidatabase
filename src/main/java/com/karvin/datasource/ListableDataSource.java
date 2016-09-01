package com.karvin.datasource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by karvin on 16/8/16.
 */
public class ListableDataSource implements DataSource,Iterable<DataSource>{

    private List<DataSource> dataSources;
    private Random random = new Random();

    public static final ListableDataSource EMPTY_LISTABLE_DATASOURCE = new ListableDataSource(Collections.EMPTY_LIST);

    public boolean isEmpty(){
        return dataSources == null || dataSources.size() == 0;
    }

    public ListableDataSource(List<DataSource> dataSources){
        this.dataSources = dataSources;
    }

    public DataSource getByIndex(int index){
        if(index>this.dataSources.size()){
            throw new IllegalArgumentException("cant get datasource size:"+dataSources.size()+" index:"+index);
        }
        return this.dataSources.get(index);
    }

    public Connection getConnection() throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).getConnection();
    }

    public Connection getConnection(String username, String password) throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).getConnection(username,password);
    }

    public PrintWriter getLogWriter() throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        int index = random.nextInt(dataSources.size());
        dataSources.get(index).setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        int index = random.nextInt(dataSources.size());
        dataSources.get(index).setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).getParentLogger();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        int index = random.nextInt(dataSources.size());
        return dataSources.get(index).isWrapperFor(iface);
    }

    public Iterator iterator() {
        return dataSources.iterator();
    }
}
