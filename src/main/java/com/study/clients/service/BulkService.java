/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients.service;

import java.io.IOException;
import java.util.Arrays;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

/**
 * TODO 批量es操作
 *
 * @author Leo
 * @version 1.0 2023/1/16
 */
public class BulkService {


    public static void bulkCreateDocument(RestHighLevelClient client) throws IOException {
        BulkRequest request = new BulkRequest("user");
        request.add(new IndexRequest().source(XContentType.JSON, "name", "leo", "sex", "男"));
        request.add(new IndexRequest().source(XContentType.JSON, "name", "wangwu", "sex", "男"));
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + Arrays.toString(responses.getItems()));
    }

    public static void bulkRemoveDocument(RestHighLevelClient client) throws IOException {
        BulkRequest request = new BulkRequest("user");
        request.add(new DeleteRequest().id("ClLWuYUBjv24Amh-588r"));
        request.add(new DeleteRequest().id("CVLWuYUBjv24Amh-588r"));
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + responses.getItems());
    }

    public static void bulkUpdateDocument(RestHighLevelClient client) throws IOException {
        BulkRequest request = new BulkRequest("user");
        request.add(new UpdateRequest().id("CFLWuYUBjv24Amh-xc_J")
            .doc(XContentType.JSON, "age", 21));
        request.add(new UpdateRequest().id("B1LWuYUBjv24Amh-xc_J")
            .doc(XContentType.JSON, "age", 23));
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + Arrays.toString(responses.getItems()));
    }
}
