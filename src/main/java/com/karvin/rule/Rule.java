package com.karvin.rule;

import com.karvin.common.ShardKey;

/**
 * Created by karvin on 16/8/16.
 */
public interface Rule {

    int decideDataSourceIndex(ShardKey shardKey);

    String decideTableName(ShardKey shardKey);

}
