package com.atguigu.reader;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {

    public static void main(String[] args) throws IOException {

        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200")
                .build();
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //4.查询数据
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //4.1 添加查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //添加全值匹配条件
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "0621");
        boolQueryBuilder.filter(termQueryBuilder);
        //添加分词匹配条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //4.2 添加聚合组
        //添加最小值聚合组
        MinAggregationBuilder minAgeGroup = AggregationBuilders.min("minAge").field("age");
        searchSourceBuilder.aggregation(minAgeGroup);

        //添加分桶聚合组
        TermsAggregationBuilder byGender = AggregationBuilders.terms("countByGender").field("gender");
        searchSourceBuilder.aggregation(byGender);

        //4.3 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student2")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);

        //5.解析数据
        //5.1 获取总数
        Long total = result.getTotal();
        System.out.println("总命中：" + total + "条数据！");

        //5.2 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            System.out.println("**************");
            for (Object o : source.keySet()) {
                System.out.println("Key:" + o + ",Value:" + source.get(o));
            }
        }

        //5.3 解析聚合组
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            System.out.println(entry.getKeyAsString() + ":" + entry.getCount());
        }

        MinAggregation minAge = aggregations.getMinAggregation("minAge");
        System.out.println(minAge.getMin());

        //6.关闭客户端
        jestClient.shutdownClient();

    }
}
