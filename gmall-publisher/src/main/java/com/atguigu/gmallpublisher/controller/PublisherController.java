package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.创建集合用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.获取新增日活
        Integer dauTotal = publisherService.getDauTotal(date);

        //3.封装新增日活的Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.封装新增设备的Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //5.将两个Map放入List
        result.add(dauMap);
        result.add(newMidMap);

        //将result转换为字符串输出
        return JSONObject.toJSONString(result);
    }


    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {

        //1.获取当天的日活分时数据
        Map todayMap = publisherService.getDauTotalHourMap(date);

        //2.获取昨天的日活分时数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        //3.创建Map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //4.将两个Map放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5.返回结果
        return JSONObject.toJSONString(result);
    }

}
