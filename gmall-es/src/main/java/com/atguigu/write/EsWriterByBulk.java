package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByBulk {

    public static void main(String[] args) throws IOException {

        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200")
                .build();
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //4.准备数据
        Movie movie1 = new Movie("1004", "金刚大战王义");
        Movie movie2 = new Movie("1005", "王义大战金刚");

        Index index1 = new Index.Builder(movie1).id("1004").build();
        Index index2 = new Index.Builder(movie2).id("1005").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_test2")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        //5.写入数据
        jestClient.execute(bulk);

        //6.关闭客户端
        jestClient.shutdownClient();

    }
}
