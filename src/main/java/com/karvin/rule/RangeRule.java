package com.karvin.rule;

import com.karvin.common.ShardKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by karvin on 16/8/16.
 */
public class RangeRule extends AbstractRule {

    private List<RangeSchema> schemas = new ArrayList<RangeSchema>();

    public RangeRule(String table) {
        super(table);
    }

    public int decideDataSourceIndex(ShardKey shardKey) {
        if(schemas == null || schemas.size() == 0){
            throw new IllegalArgumentException("schemas should not be empty");
        }
        for(RangeSchema schema:schemas){
            long value = (Long)shardKey.getValue();
            if(isIn(value,schema)){
                return schema.index;
            }
        }
        throw new IllegalArgumentException("get no schema");
    }

    public String decideTableName(ShardKey shardKey) {
        if(schemas == null || schemas.size() == 0){
            throw new IllegalArgumentException("schemas should not be empty");
        }
        for(RangeSchema schema:schemas){
            long value = (Long)shardKey.getValue();
            if(isIn(value,schema)){
                return this.getTable()+"_"+schema.tableIndex;
            }
        }
        throw new IllegalArgumentException("get no table");
    }

    public void addSchemaRule(RangeSchema schema){
        if(this.schemas == null){
            this.schemas = new ArrayList<RangeSchema>();
        }
        this.schemas.add(schema);
    }

    public static class RangeSchema{
        private int index;
        private int tableIndex;
        private long start;
        private long end;

        public RangeSchema( int index, int tableIndex,long start,long end) {
            this.end = end;
            this.index = index;
            this.start = start;
            this.tableIndex = tableIndex;
        }
    }

    private boolean isIn(long value,RangeSchema rule){
        return value>=rule.start&&value<=rule.end;
    }
}
