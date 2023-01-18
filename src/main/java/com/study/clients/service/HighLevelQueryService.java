/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients.service;

import java.io.IOException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * TODO es 高级查询
 *
 * @author Leo
 * @version 1.0 2023/1/17
 */
public class HighLevelQueryService {

    private static void commonPrint(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<<========");
    }


    /**
     * es search by match all
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByAll(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        commonPrint(response);
    }

    /**
     * es search by term query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByTerm(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder age = QueryBuilders.termQuery("age", 212);
        searchSourceBuilder.query(age);
        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }

    /**
     * es search by page query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByPage(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        request.source(searchSourceBuilder);
        searchSourceBuilder.sort("age", SortOrder.DESC);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }

    /**
     * es search by filter field
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByFilterField(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        String[] includes = {"name"};
        String[] excludes = {};
        builder.query(QueryBuilders.matchAllQuery());
        builder.fetchSource(includes, excludes);
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }


    /**
     * es search by bool query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByBoolQuery(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "jjcc"));
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("age", 26));
        boolQueryBuilder.should();

        builder.query(boolQueryBuilder);
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }

    /**
     * es search by range query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByRangeQuery(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder range = new RangeQueryBuilder("age");
        range.lte(500);
        range.gt(1);

        builder.query(range);

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }

    /**
     * es search by fuzzy query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByFuzzyQuery(RestHighLevelClient client) throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("name","wangwu").fuzziness(Fuzziness.ONE));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        commonPrint(response);
    }

    /**
     * es search by group by query
     * @author Leo
     * @param client es client
     */
    public static void searchDocumentByAggsQuery(RestHighLevelClient client) throws IOException {
        // 高亮查询
        SearchRequest request = new SearchRequest().indices("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // max()
        // sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        // count()
        sourceBuilder.aggregation(AggregationBuilders.count("count").field("age"));
        // 分组 count()
        sourceBuilder.aggregation(AggregationBuilders.terms("terms").field("age"));


        sourceBuilder.size(0);
        //设置请求体
        request.source(sourceBuilder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.打印响应结果
        SearchHits hits = response.getHits();
        System.out.println(response);
    }

}
