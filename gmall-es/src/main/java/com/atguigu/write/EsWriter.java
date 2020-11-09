package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {

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
        Movie movie = new Movie("1003", "金刚川2");
        Index index = new Index.Builder(movie)
                .index("movie_test2")
                .type("_doc")
                .id("1003")
                .build();

        //5.写入数据
        jestClient.execute(index);

        //6.关闭客户端
        jestClient.shutdownClient();

    }
}
