/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients;

import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.clients.service.HighLevelQueryService;
import com.study.dto.User;
import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.xcontent.XContentType;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/1/13
 */
public class ElasticsearchByRestHighLevelClient01 {



    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        try (RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        )) {
            // createIndex(client);
            // getIndex(client);
            // removeIndex(client);
            // createDocument(client);
            // updateDocument(client);
            // getDocumentById(client);
            // removeDocumentById(client);
            // BulkService.bulkCreateDocument(client);
            // BulkService.bulkRemoveDocument(client);
            // BulkService.bulkUpdateDocument(client);

            // HighLevelQueryService.searchDocumentByAll(client);
            // HighLevelQueryService.searchDocumentByTerm(client);
            // HighLevelQueryService.searchDocumentByPage(client);
            // HighLevelQueryService.searchDocumentByFilterField(client);
            // HighLevelQueryService.searchDocumentByBoolQuery(client);
            // HighLevelQueryService.searchDocumentByRangeQuery(client);
            // HighLevelQueryService.searchDocumentByFuzzyQuery(client);
            HighLevelQueryService.searchDocumentByAggsQuery(client);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void removeDocumentById(RestHighLevelClient client) throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    public static void getDocumentById(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest();
        request.index("user").id("1001");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("source:" + response.getSourceAsString());
    }

    public static void updateDocument(RestHighLevelClient client) throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        request.doc(XContentType.JSON, "sex", "女", "name", "jjcc");
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    public static void createDocument(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user").id("1002");
        User user = new User();
        user.setName("jjcc");
        user.setAge(26);
        user.setSex("男");
        ObjectMapper objectMapper = new ObjectMapper();
        String writeValueAsString = objectMapper.writeValueAsString(user);
        request.source(writeValueAsString, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());

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
