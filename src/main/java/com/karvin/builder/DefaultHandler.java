package com.karvin.builder;

import com.karvin.datasource.DataSourceFactory;
import org.xml.sax.Attributes;

/**
 * Created by karvin on 16/8/21.
 */
public class DefaultHandler implements ElementHandler {

    private DataSourceFactory factory;

    public DefaultHandler(DataSourceFactory factory){
        this.factory = factory;
    }

    public void parse(Attributes attributes,String catalog) {
        if(attributes.getLength()>0){
            String catalogE = attributes.getValue("catalog");
            if(catalog == null){
                catalog = catalogE;
            }
            String url = attributes.getValue("url");
            String username = attributes.getValue("username");
            String password = attributes.getValue("password");
            String masterString = attributes.getValue("master");
            String indexString = attributes.getValue("index");
            DataSourceFactory.ConfigSection section = new DataSourceFactory.ConfigSection(catalog,url,username,password,masterString,indexString);
            factory.sections.add(section);
        }
    }
}
