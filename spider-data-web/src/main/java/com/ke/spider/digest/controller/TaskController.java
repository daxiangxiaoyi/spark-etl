package com.ke.spider.digest.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhaozhuo
 * @date 2020/4/28
 */
@RestController
@RequestMapping("/v1/spider/data/etl/task")
public class TaskController {

    @RequestMapping("/hello")
    public String say() {
        return "Hello ,this is a demo";
    }
}
