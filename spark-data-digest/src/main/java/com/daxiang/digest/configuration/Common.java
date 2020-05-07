package com.daxiang.digest.configuration;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class Common {
    private static final List<String> allowedModes = Arrays.asList(new String[]{"client", "cluster"});
    private String mode;

    public Boolean isModeAllowed(String mode) {

        return allowedModes.contains(mode.toLowerCase());
    }

    public String getDeployMode() {
        return this.mode;
    }

    public Boolean setDeployMode(String mode) {
        Boolean allowed = false;
        if (isModeAllowed(mode)) {
            this.mode = mode;
            allowed = true;
        }
        return allowed;

    }
}
