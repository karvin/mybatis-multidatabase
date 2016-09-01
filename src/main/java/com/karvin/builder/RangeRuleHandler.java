package com.karvin.builder;

import com.karvin.rule.RangeRule;
import com.karvin.rule.Rule;
import com.karvin.rule.RuleInstance;
import org.xml.sax.Attributes;

/**
 * Created by karvin on 16/8/22.
 */
public class RangeRuleHandler implements ElementHandler
{
    public void parse(Attributes attributes, String catalog) {
        if(attributes.getLength()>0) {
            String catalogE = attributes.getValue("catalog");
            if (catalog == null) {
                catalog = catalogE;
            }
            Rule rule = RuleInstance.getInstance().getRule(catalog);
            if (rule == null) {
                String table = attributes.getValue("table");
                rule = new RangeRule(table);
                RuleInstance.getInstance().addRule(catalog, rule);
            }
            String start = attributes.getValue("start");
            String end = attributes.getValue("end");
            String indexString = attributes.getValue("index");
            String tableIndex = attributes.getValue("tableIndex");
            ((RangeRule)rule).addSchemaRule(this.getSchema(indexString,tableIndex,start,end));
        }
    }

    private RangeRule.RangeSchema getSchema(String index,String tableIndex,String start,String end){
        RangeRule.RangeSchema schema = null;
        try{
            int iIndex = Integer.parseInt(index);
            int iTableIndex = Integer.parseInt(tableIndex);
            Long lStart = Long.parseLong(start);
            Long lEnd = Long.parseLong(end);
            schema = new RangeRule.RangeSchema(iIndex,iTableIndex,lStart,lEnd);
            return schema;
        }catch (Exception e){
            throw new IllegalArgumentException("failed to parse schema");
        }
    }
}
