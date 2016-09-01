package com.karvin.datasource;

import com.karvin.builder.*;
import com.karvin.plugin.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by karvin on 16/8/19.
 */
public class DataSourceFactory implements FactoryBean {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);
    public Set<ConfigSection> sections = new HashSet<ConfigSection>();

    public DataSourceFactory(){
        ElementHandlers.getInstance().registerHandler("section", new DefaultHandler(this));
        ElementHandlers.getInstance().registerHandler("modRule", new ModRuleHandler());
        ElementHandlers.getInstance().registerHandler("rangeRule",new RangeRuleHandler());
    }

    public Object getObject() throws Exception {
        this.init();
        DataSourceMapper mapper = new DataSourceMapper();
        Map<String,List<ConfigSection>> map = this.getSectionMap();
        for(Map.Entry<String,List<ConfigSection>> entry:map.entrySet()){
            DataSource dataSource = this.createDataSource(entry.getValue());
            mapper.getDataSourceMap().put(entry.getKey(),dataSource);
        }
        return mapper;
    }

    private DataSource createDataSource(List<ConfigSection> sections) throws Exception {
        if(CollectionUtils.isEmpty(sections)){
            return null;
        }
        if(sections.size() == 1){
            SimpleDataSource dataSource = new SimpleDataSource();
            dataSource.setDataSource(this.create(sections.get(0)));
            return dataSource;
        }
        return this.create(sections);
    }

    private Map<String,List<ConfigSection>> getSectionMap(){
        if(CollectionUtils.isEmpty(this.sections)){
            return Collections.EMPTY_MAP;
        }
        Map<String,List<ConfigSection>> map = new HashMap<String, List<ConfigSection>>();
        for(ConfigSection section:sections){
            List<ConfigSection> list = map.get(section.getCatalog());
            if(list == null){
                list = new ArrayList<ConfigSection>();
            }
            list.add(section);
            map.put(section.getCatalog(),list);
        }
        return map;
    }

    public Class<?> getObjectType() {
        return DataSource.class;
    }

    public boolean isSingleton() {
        return true;
    }

    private void init(){
        String location = System.getProperty("com.karvin.datasource.config");
        InputStream stream = this.getStream(location);
        try{
            this.parseSections(stream);
        }finally {
            try {
                stream.close();
            } catch (IOException e) {

            }
        }
    }

    private InputStream getStream(String location){
        InputStream stream = null;
        File file = new File(location);
        if(file.exists()){
            try {
                stream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        if(stream == null){
            try {
                ResourceLoader loader = new DefaultResourceLoader();
                stream = loader.getResource(location).getInputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return stream;
    }

    private List<ConfigSection> parseSections(InputStream stream){
        List<ConfigSection> sections = new ArrayList<ConfigSection>();
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = parserFactory.newSAXParser();
            parser.parse(stream,new MyHandler());
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sections;
    }

    private class MyHandler extends org.xml.sax.helpers.DefaultHandler {

        private Attributes attributes;
        private String catalog;

        public void startElement (String uri, String localName,
                                  String qName, Attributes attributes)
                throws SAXException
        {
            if(qName.equals("sections")){
                if(attributes.getLength()>0){
                    catalog = attributes.getValue("catalog");
                }
                return;
            }
            if(qName.equals("rules")){
                if(attributes.getLength()>0){
                    catalog = attributes.getValue("catalog");
                }
                return;
            }
            this.attributes = attributes;
        }

        public void endElement (String uri, String localName, String qName)
                throws SAXException
        {
            ElementHandler handler = ElementHandlers.getInstance().getHandler(qName);
            if(handler != null){
                handler.parse(this.attributes,this.catalog);
            }
        }
    }

    public static class ConfigSection{
        private String catalog;
        private String url;
        private String username;
        private String password;
        private boolean master=true;
        private int index;

        public ConfigSection(String catalog,String url,String username,String password,String masterString,String indexString){
            this.catalog = catalog;
            this.url = url;
            this.username = username;
            this.password = password;
            if(!StringUtils.isEmpty(masterString)){
                try{
                    this.master = Boolean.parseBoolean(masterString);
                }catch (Exception e){

                }
            }
            if(!StringUtils.isEmpty(indexString)){
                try{
                    this.index = Integer.parseInt(indexString);
                }catch (Exception e){

                }
            }
        }

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public boolean isMaster() {
            return master;
        }

        public void setMaster(boolean master) {
            this.master = master;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }
    }

    private DataSource create(ConfigSection section) throws Exception {
        String className = System.getProperty("com.karvin.shard.datasource.class","");
        Class clazz = this.loadClass(className);
        if(clazz == null){
            throw new IllegalArgumentException("class:"+className +" not exist");
        }

        Constructor constructor = clazz.getConstructor();
        Object object = constructor.newInstance();
        this.setPropertyValue("jdbcUrl", object, section.getUrl());
        this.setPropertyValue("user", object, section.getUsername());
        this.setPropertyValue("password", object, section.getPassword());
        return (DataSource)object;
    }

    private void setPropertyValue(String property,Object target,Object value) throws InvocationTargetException {
        try {
            ReflectionUtils.setFieldValue(target, property, value);
        }catch (Exception e){
            try {
                ReflectionUtils.invokeMethod(target,this.getMethodName(property),new Class[]{value.getClass()},new Object[]{value});
            } catch (InvocationTargetException e1) {
                throw e1;
            }
        }
    }

    private String getMethodName(String property){
        StringBuilder sb = new StringBuilder();
        sb.append("set");
        sb.append(property.substring(0,1).toUpperCase());
        sb.append(property.substring(1));
        return sb.toString();
    }

    private Class loadClass(String className){
        Class clazz = null;
        try {
            clazz = ClassLoader.getSystemClassLoader().loadClass(className);
        }catch (Exception e){

        }
        if(clazz == null){
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {

            }
        }
        if(clazz == null){
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {

            }
        }
        return clazz;
    }

    private DataSource create(List<ConfigSection> sections) throws Exception {
        List<DataSource> dataSources = new ArrayList<DataSource>();
        if(isMasterSlave(sections)){
            List<SectionGroup> groups = this.buildGroups(sections);
            for(SectionGroup group:groups){
                if(group.masterSection == null){
                    continue;
                }
                DataSource masterDataSource = create(group.getMasterSection());
                ListableDataSource slaveDataSource = null;
                List<DataSource> listDataSource = new ArrayList<DataSource>();
                if(group.slaveSection != null && group.slaveSection.size()>0){
                    for(ConfigSection section:group.slaveSection){
                        listDataSource.add(create(section));
                    }
                    slaveDataSource = new ListableDataSource(listDataSource);
                }
                MasterSlaveDataSource dataSource = new MasterSlaveDataSource(masterDataSource,slaveDataSource);
                dataSources.add(dataSource);
            }
        }else{
            for(ConfigSection section:sections){
                DataSource dataSource = create(section);
                dataSources.add(dataSource);
            }
        }
        return new ListableDataSource(dataSources);
    }

    private boolean isMasterSlave(List<ConfigSection> sections){
        if(CollectionUtils.isEmpty(sections)){
            return false;
        }
        for(ConfigSection section:sections){
            if(section.isMaster()){
                return true;
            }
        }
        return false;
    }

    private List<SectionGroup> buildGroups(List<ConfigSection> sections){
        if(CollectionUtils.isEmpty(sections)){
            return Collections.EMPTY_LIST;
        }
        Map<Integer,List<ConfigSection>> map = new HashMap<Integer, List<ConfigSection>>();
        for(ConfigSection section:sections){
            List<ConfigSection> list = map.get(section.getIndex());
            if(list == null){
                list = new ArrayList<ConfigSection>();
            }
            list.add(section);
            map.put(section.getIndex(),list);
        }
        if(CollectionUtils.isEmpty(map)){
            return Collections.EMPTY_LIST;
        }
        List<SectionGroup> groups = new ArrayList<SectionGroup>();
        for(Map.Entry<Integer,List<ConfigSection>> entry:map.entrySet()){
            SectionGroup group = new SectionGroup(entry.getValue());
            groups.add(group);
        }
        return groups;
    }

    private class SectionGroup{
        private ConfigSection masterSection;
        private List<ConfigSection> slaveSection = new ArrayList<ConfigSection>();

        public SectionGroup(List<ConfigSection> sections){
            if(CollectionUtils.isEmpty(sections)){
                throw new IllegalArgumentException("sections is invalid");
            }
            for(ConfigSection section:sections){
                if(section.isMaster()){
                    if(masterSection != null){
                        throw new IllegalArgumentException("only one master allowed");
                    }else{
                        this.masterSection = section;
                    }
                }else{
                    this.slaveSection.add(section);
                }
            }
            if(this.masterSection == null){
                if(this.slaveSection.size() == 1){
                    this.masterSection = this.slaveSection.get(0);
                    this.slaveSection.clear();
                }
            }
        }

        public ConfigSection getMasterSection() {
            return masterSection;
        }

        public void setMasterSection(ConfigSection masterSection) {
            this.masterSection = masterSection;
        }

        public List<ConfigSection> getSlaveSection() {
            return slaveSection;
        }

        public void setSlaveSection(List<ConfigSection> slaveSection) {
            this.slaveSection = slaveSection;
        }
    }

    public static void main(String[] args){
        System.setProperty("com.karvin.datasource.config","/data/datasource.xml");
        System.setProperty("com.karvin.shard.datasource.class","com.mchange.v2.c3p0.ComboPooledDataSource");
        DataSourceFactory factory = new DataSourceFactory();
        try {
            Class clazz = factory.loadClass("com.karvin.datasource.DataSourceFactory");
            factory.getObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
