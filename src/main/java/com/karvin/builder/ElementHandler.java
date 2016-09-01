package com.karvin.builder;

import org.xml.sax.Attributes;

/**
 * Created by karvin on 16/8/21.
 */
public interface ElementHandler {

    void parse(Attributes attributes,String catalog);

}
