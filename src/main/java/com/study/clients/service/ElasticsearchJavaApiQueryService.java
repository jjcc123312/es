/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.study.dto.UserNew;
import java.io.IOException;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import co.elastic.clients.elasticsearch._types.SortOrder;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/2/1
 */
@Log4j2
public class ElasticsearchJavaApiQueryService {

    private static <T> void commonPrint(SearchResponse<T> response, Class<T> tClass) {
        HitsMetadata<T> hits = response.hits();

        System.out.println("took:" + response.took());
        System.out.println("timeout:" + response.timedOut());
        System.out.println("total:" + hits.total());
        System.out.println("MaxScore:" + hits.maxScore());
        System.out.println("hits========>>");
        for (Hit<T> hit : hits.hits()) {
            String s = hit.toString();
            System.out.println(s);
        }
        System.out.println("<<========");
    }


    /**
     * query all document
     *
     * @param client client
     */
    public static void queryAllDoc(ElasticsearchClient client) throws IOException {
        SearchResponse<UserNew> response = client.search(fn -> fn.index("user_new")
            .query(query -> query.matchAll(m -> m)), UserNew.class);

        commonPrint(response, UserNew.class);
    }

    /**
     * query document by term
     *
     * @param client client
     * @author Leo
     */
    public static void searchDocumentByTerm(ElasticsearchClient client) throws IOException {
        SearchResponse<UserNew> response = client.search(fn -> {
            return fn.index("user_new").query(query -> {
                return query.term(term -> term.field("sex").value("男"));
            });
        }, UserNew.class);

        commonPrint(response, UserNew.class);
    }

    /**
     * query document by terms
     *
     * @param client client
     * @author Leo
     */
    public static void searchDocumentByTerms(ElasticsearchClient client) throws IOException {
        SearchResponse<UserNew> response = client.search(fn -> fn.index("user_new").query(query -> query.terms(terms -> {
            FieldValue fieldValue1 = FieldValue.of("leo");
            FieldValue fieldValue2 = FieldValue.of("zhangsan");

            return terms.field("name").terms(t -> t.value(List.of(fieldValue1, fieldValue2)));
        })), UserNew.class);

        commonPrint(response, UserNew.class);
    }

    /**
     * page query document
     * @param client client
     */
    public static void searchDocByPage(ElasticsearchClient client) throws IOException {
        SearchResponse<UserNew> response = client.search(fn ->
                fn.index("user_new").query(query ->
                    query.bool(bool ->
                        bool.must(must ->
                            must.term(TermQuery.of(termQuery ->
                                termQuery.field("sex").value("女")
                            ))
                        )
                    )
                ).from(0).size(2)
                    .sort(sort ->
                        sort.field(field
                            -> field.field("age").order(SortOrder.Asc)))
            , UserNew.class);

        commonPrint(response, UserNew.class);
    }


    public static void searchDocumentByFilterField(ElasticsearchClient client) throws IOException {
        SearchResponse<UserNew> response = client.search(
            fn -> fn.index("user_new").source(source -> source.filter(filter -> filter.excludes("age"))), UserNew.class);
        commonPrint(response, UserNew.class);
    }

    public static void searchDocumentByBoolQuery(ElasticsearchClient client) throws IOException {
        client.search(fn -> fn.index("user_new").query(query -> query.bool(bool ->
            bool.must(must -> must.term( term -> term.field("").value("")))
                .must(must -> must.term( term -> term.field("").value("")))
                .must(must -> must.fuzzy(fuzzy -> fuzzy.field("").value("").fuzziness("2")))
            )), UserNew.class);
    }

}
