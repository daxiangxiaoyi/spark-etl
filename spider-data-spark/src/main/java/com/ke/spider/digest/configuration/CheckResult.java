package com.ke.spider.digest.configuration;

import lombok.Data;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
@Data
public class CheckResult {
    private boolean success;

    private String msg;

    public CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isSuccess() {
        return success;
    }


}
