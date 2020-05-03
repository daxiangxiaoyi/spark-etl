package com.ke.spider.digest.configuration;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class ConfigPackage {
    private String engine;
    public  String packagePrefix ;
    public  String  upperEnginx ;
    public  String  sourcePackage ;
    public  String  transformPackage ;
    public  String  sinkPackage ;
    public  String  envPackage ;
    public  String  baseSourcePackage ;
    public  String  baseTransformPackage;
    public  String  baseSinkPackage ;
    public  String  pluginPackagePrefix ;

    public ConfigPackage(String engine){
        this.engine=engine;
        this.packagePrefix = "com.ke.spider.digest.framework." + engine;
        this.pluginPackagePrefix = "com.ke.spider.digest" ;
        this.upperEnginx = engine.substring(0,1).toUpperCase() + engine.substring(1);
        this.sourcePackage = pluginPackagePrefix + ".source";
        this.transformPackage = pluginPackagePrefix + ".transform";
        this.sinkPackage = pluginPackagePrefix + ".sink";
        this.envPackage = packagePrefix + ".env";
        this.baseSourcePackage = packagePrefix + ".Abstract" + upperEnginx + "Source";
        this.baseTransformPackage = packagePrefix + ".Abstract" + upperEnginx + "Transform";
        this.baseSinkPackage = packagePrefix + ".Abstract" + upperEnginx + "Sink";
    }

}
