package com.karvin.rule;

/**
 * Created by karvin on 16/8/16.
 */
public abstract class AbstractRule implements Rule{

    private String table;

    public AbstractRule(String table){
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

}
