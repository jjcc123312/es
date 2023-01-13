/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/1/13
 */
public class ElasticsearchByRestHighLevelClient01 {

    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        try {
            createIndex(client);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }


    }


    public static void getIndex() {

    }

    public static void removeIndex(RestHighLevelClient client) {

    }


    /**
     * create index
     * @author Leo
     * @param client es client
     */
    public static void createIndex(RestHighLevelClient client) throws IOException {
        // 创建索引 - 请求对象
        CreateIndexRequest request = new CreateIndexRequest("user");
        // 发送请求，获取响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        // 响应状态
        System.out.println("操作状态 = " + acknowledged);
    }

}
