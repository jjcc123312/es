/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.study.clients.service.ElasticsearchJavaApiService;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/1/30
 */
public class ElasticsearchByRestClient01 {

    public static void main(String[] args){
        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
            .build();
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper())) {
            ElasticsearchClient client = new ElasticsearchClient(transport);

            // ElasticsearchJavaApiService.createIndex(client);
            // ElasticsearchJavaApiService.removeIndex(client);
            // ElasticsearchJavaApiService.getIndex(client);
            // ElasticsearchJavaApiService.settingMapping(client);
            // ElasticsearchJavaApiService.createDoc(client);
            // ElasticsearchJavaApiService.bulkCreateDoc(client);
            // ElasticsearchJavaApiService.updateDocById(client);
            // ElasticsearchJavaApiService.getDocById(client);
            ElasticsearchJavaApiService.removeDocById(client);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
