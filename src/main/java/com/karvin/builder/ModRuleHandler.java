package com.karvin.builder;

import com.karvin.rule.Rule;
import com.karvin.rule.RuleInstance;
import com.karvin.rule.ShardModRule;
import org.xml.sax.Attributes;

/**
 * Created by karvin on 16/8/22.
 */
public class ModRuleHandler implements ElementHandler {
    public void parse(Attributes attributes, String catalog) {
        if(attributes.getLength()>0) {
            String catalogE = attributes.getValue("catalog");
            if (catalog == null) {
                catalog = catalogE;
            }
            Rule rule = RuleInstance.getInstance().getRule(catalog);
            if (rule == null) {
                String modString = attributes.getValue("mod");
                String table = attributes.getValue("table");
                rule = new ShardModRule(table, modString);
                RuleInstance.getInstance().addRule(catalog, rule);
            }
            String start = attributes.getValue("start");
            String end = attributes.getValue("end");
            String indexString = attributes.getValue("index");
            ((ShardModRule) rule).addSchemaRule(this.getSchemaRule(indexString, start, end));
        }
    }

    private ShardModRule.SchemaRule getSchemaRule(String index,String start,String end){
        int iIndex = Integer.parseInt(index);
        long iStart = Long.parseLong(start);
        long iEnd = Long.parseLong(end);
        ShardModRule.SchemaRule rule = new ShardModRule.SchemaRule(iIndex,iStart,iEnd);
        return rule;
    }
}
