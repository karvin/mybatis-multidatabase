package com.karvin.builder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by karvin on 16/8/21.
 */
public class ElementHandlers {

    private static Map<String,ElementHandler> handlerMap = new ConcurrentHashMap<String, ElementHandler>();

    private static ElementHandlers elementHandlers = new ElementHandlers();

    private ElementHandlers(){

    }

    public static ElementHandlers getInstance(){
        return elementHandlers;
    }

    public void registerHandler(String elementName,ElementHandler elementHandler){
        handlerMap.put(elementName,elementHandler);
    }

    public ElementHandler getHandler(String element){
        return handlerMap.get(element);
    }

}
