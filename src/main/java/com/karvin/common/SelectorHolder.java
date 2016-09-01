package com.karvin.common;

/**
 * Created by karvin on 16/8/16.
 */
public class SelectorHolder {

    public static ThreadLocal<String> database = new ThreadLocal<String>();
    public static ThreadLocal<ShardKey> key = new ThreadLocal<ShardKey>();

}
