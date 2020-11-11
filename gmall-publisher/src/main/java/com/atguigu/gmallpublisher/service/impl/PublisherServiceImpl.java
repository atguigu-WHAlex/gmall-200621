package com.atguigu.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.avro.data.Json;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoenix获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回数据
        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //1.查询Phoenix获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回数据
        return result;
    }

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);
        System.out.println(searchSourceBuilder.toString());

        //创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();

        //执行查询操作
        SearchResult searchResult = jestClient.execute(search);

        //创建Map用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        //获取总数
        Long total = searchResult.getTotal();

        //获取明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        MetricAggregation aggregations = searchResult.getAggregations();
        //获取性别聚合组数据
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        Long maleCount = 0L;
        for (TermsAggregation.Entry entry : groupby_user_gender.getBuckets()) {
            if (entry.getKey().equals("M")) {
                maleCount = entry.getCount();
            }
        }
        //计算male的占比
        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        Double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //封装性别的Option对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别占比的集合
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);

        //创建性别饼图数据对象
        Stat genderStat = new Stat("用户性别占比", genderOptions);

        //获取年龄聚合组数据
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        Long lower20 = 0L;
        Long upper30 = 0L;
        for (TermsAggregation.Entry entry : groupby_user_age.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 += entry.getCount();
            } else if (Integer.parseInt(entry.getKey()) >= 30) {
                upper30 += entry.getCount();
            }
        }

        //计算年龄占比
        Double lower20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        Double upper30Ratio = Math.round(upper30 * 1000D / total) / 10D;
        Double upper20to30 = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;

        //创建年龄的Option对象
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30);
        Option upper30RatioOpt = new Option("30岁及30岁以上", upper30Ratio);

        //创建集合用于存放年龄Option对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30RatioOpt);

        //创建年龄的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        //创建饼图所需数据的集合对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //将总数,明细数据,聚合组数据存放至Map
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        //将集合转换为字符串返回
        return JSON.toJSONString(result);
    }

}
