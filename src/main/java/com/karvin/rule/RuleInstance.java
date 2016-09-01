package com.karvin.rule;

import com.karvin.common.SelectorHolder;
import com.karvin.common.ShardKey;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karvin on 16/8/18.
 */
public class RuleInstance {

    private  static RuleInstance instance = new RuleInstance();

    private Map<String,Rule> map = new ConcurrentHashMap<String, Rule>();

    private RuleInstance(){

    }

    public static RuleInstance getInstance(){
        return instance;
    }

    public void addRule(String catalog,Rule rule){
        map.put(catalog,rule);
    }

    public Rule getRule(String catalog){
        return map.get(catalog);
    }

    public Rule match(){
        ShardKey key = SelectorHolder.key.get();
        String catalog = SelectorHolder.database.get();
        if(key == null || StringUtils.isEmpty(catalog)) {
            return null;
        }
        Rule rule = map.get(catalog);
        return rule;
    }

}
