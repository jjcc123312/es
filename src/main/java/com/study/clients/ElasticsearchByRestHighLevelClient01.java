/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/1/13
 */
public class ElasticsearchByRestHighLevelClient01 {



    public static void main(String[] args) throws IOException {
        // private static Logger logger = Logger.getLogger(ElasticsearchByRestHighLevelClient01.class);
        // 创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        try {
            createIndex(client);
            // getIndex(client);
            // removeIndex(client);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void createDocument() {


    }

    public static void getIndex(RestHighLevelClient client) throws IOException {
        GetIndexRequest request = new GetIndexRequest("user");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        System.out.println("aliases:"+response.getAliases());
        System.out.println("mappings:"+response.getMappings());
        System.out.println("settings:"+response.getSettings());
    }

    public static void removeIndex(RestHighLevelClient client) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
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
