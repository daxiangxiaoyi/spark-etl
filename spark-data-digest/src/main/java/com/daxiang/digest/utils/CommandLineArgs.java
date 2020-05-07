package com.daxiang.digest.utils;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class CommandLineArgs {

    private String deployMode = "client";
    private String configFile = "applicaiton.conf";
    private Boolean testConfig = false;

    CommandLineArgs(String deployMode, String configFile, Boolean testConfig) {
        this.deployMode = deployMode;
        this.configFile = configFile;
        this.testConfig = testConfig;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public Boolean getTestConfig() {
        return testConfig;
    }

    public void setTestConfig(Boolean testConfig) {
        this.testConfig = testConfig;
    }
}
