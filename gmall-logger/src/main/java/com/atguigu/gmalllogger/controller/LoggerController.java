package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")
    //    @ResponseBody
    public String test01() {
        System.out.println("11111");
        return "success";
    }

    @RequestMapping("test2")
    public String test02(@RequestParam("name") String nn,
                         @RequestParam("age") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {

        //1.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //2.将jsonObject转换为字符串
        String logStr = jsonObject.toString();

        //System.out.println(logString);
        //3.将数据落盘
        log.info(logStr);

        //4.根据数据类型发送至不同的主题
        if ("startup".equals(jsonObject.getString("type"))) {
            //启动日志
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP, logStr);
        } else {
            //事件日志
            kafkaTemplate.send(GmallConstant.GMALL_EVENT, logStr);
        }

        return "success";
    }

}