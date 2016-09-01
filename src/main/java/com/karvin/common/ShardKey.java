package com.karvin.common;

/**
 * Created by karvin on 16/8/16.
 */
public class ShardKey<T extends Comparable> {

    private boolean useMaster;
    private T value;

    public boolean isUseMaster() {
        return useMaster;
    }

    public void setUseMaster(boolean useMaster) {
        this.useMaster = useMaster;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
