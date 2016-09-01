package com.karvin.rule;

import com.karvin.common.ShardKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by karvin on 16/8/16.
 */
public class ShardModRule extends AbstractRule {

    private int mod;
    private List<SchemaRule> schemaRules;

    public ShardModRule(String table) {
        super(table);
    }

    public ShardModRule(String table,int mod){
        this(table);
        this.mod = mod;
    }

    public ShardModRule(String table,String mod){
        this(table);
        try {
            this.mod = Integer.parseInt(mod);
        }catch (Exception e){
            throw new IllegalArgumentException("illegal argument mod");
        }
    }

    public int getMod() {
        return mod;
    }

    public void setMod(int mod) {
        this.mod = mod;
    }

    public List<SchemaRule> getSchemaRules() {
        return schemaRules;
    }

    public void setSchemaRules(List<SchemaRule> schemaRules) {
        this.schemaRules = schemaRules;
    }

    public void addSchemaRule(SchemaRule schemaRule){
        if(this.schemaRules == null){
            this.schemaRules = new ArrayList<SchemaRule>();
        }
        this.schemaRules.add(schemaRule);
    }

    public int decideDataSourceIndex(ShardKey shardKey) {
        if(schemaRules == null){
            throw new IllegalArgumentException("no schema rules");
        }
        for(SchemaRule schemaRule:schemaRules){
            if(isIn((Long)shardKey.getValue()%this.mod,schemaRule)){
                return schemaRule.index;
            }
        }
        throw new IllegalArgumentException("can not decide index");
    }

    public String decideTableName(ShardKey shardKey) {
        long value = (Long)shardKey.getValue();
        int index = (int)(value%mod);
        return this.getTable()+"_"+index;
    }

    public static class SchemaRule{
        private int index;
        private Comparable start;
        private Comparable end;

        public SchemaRule(int index,Comparable start,Comparable end){
            this.index = index;
            this.start = start;
            this.end = end;
        }
    }

    private boolean isIn(Comparable comparable,SchemaRule rule){
        if(comparable == null || rule == null){
            return false;
        }
        return comparable.compareTo(rule.start)>=0 && comparable.compareTo(rule.end)<0;
    }
}
