/*
 * Copyright(c) 2022 长沙市希尚网络科技有限公司
 * 注意：本内容仅限于长沙市希尚网络科技有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */

package com.study.clients.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation.Builder;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import com.study.dto.UserNew;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import lombok.extern.log4j.Log4j2;

/**
 * TODO
 *
 * @author Leo
 * @version 1.0 2023/1/31
 */
@Log4j2
public class ElasticsearchJavaApiService {


    /**
     * create index
     * @author Leo
     * @param client client
     */
    public static void createIndex(ElasticsearchClient client) throws IOException {
        HashMap<String, Property> propertyHashMap = new HashMap<>();


        CreateIndexResponse response = client.indices().create(e ->
            e.index("user_new")
                .mappings(mapping -> mapping.properties(propertyHashMap))
        );
        log.info("结束新建ES索引,res={}", response.acknowledged());
    }

    /**
     * setting es index mapping
     * @author Leo
     * @param client client
     */
    public static void settingMapping(ElasticsearchClient client) throws IOException {
        HashMap<String, Property> propertyHashMap = new HashMap<>();
        propertyHashMap.put("name",
        Property.of(property -> property.text(
            TextProperty.of(textProperty
                -> textProperty.index(true))
            )
        )
        );

        propertyHashMap.put("age",
        Property.of(property -> property.integer(
            IntegerNumberProperty.of(integerNumberProperty
                -> integerNumberProperty.index(true)))));

        propertyHashMap.put("sex",
        Property.of(property -> property.keyword(
            KeywordProperty.of(keyword
                -> keyword.index(true)))));

        propertyHashMap.put("id", Property.of(property
            -> property.text(TextProperty.of(textProperty
            -> textProperty.index(true)))));

        PutMappingResponse response = client.indices().putMapping(fn ->
            fn.index(Collections.singletonList("user_new")).properties(propertyHashMap));
        log.info("结束新建ES索引,res={}", response.acknowledged());
    }

    /**
     * remove index
     * @author Leo
     * @param client client
     */
    public static void removeIndex(ElasticsearchClient client) throws IOException {
        DeleteIndexResponse response = client.indices()
            .delete(fn -> fn.index(Collections.singletonList("user_new")));
        log.info("开始删除索引,res={}", response.acknowledged());
    }

    public static void getIndex(ElasticsearchClient client) throws IOException {
        GetIndexResponse response = client.indices().get(fn ->
            fn.index(Collections.singletonList("user_new"))
        );
        log.info("response:{}", response);

    }

    public static void createDoc(ElasticsearchClient client) throws IOException {
        UserNew userNew = new UserNew();
        userNew.setSex("男");
        userNew.setName("jjcc");
        userNew.setAge(18);

        CreateResponse response = client.create(fn ->
            fn.index("user_new").id("1002").document(userNew)
        );
        log.info("result:{}", response.result());
    }

    /**
     * batch es operation
     * @author Leo
     * @param client client
     */
    public static void bulkCreateDoc(ElasticsearchClient client) throws IOException {
        List<BulkOperation> operationArrayList = new ArrayList<>();

        BulkOperation build1 = new Builder().create(d -> {
            UserNew userNew = new UserNew();
            userNew.setSex("女");
            userNew.setName("lucy");
            userNew.setAge(21);
            return d.document(userNew);
        }).build();

        BulkOperation build2 = new Builder().create(d -> {
            UserNew userNew = new UserNew();
            userNew.setSex("女");
            userNew.setName("kelly");
            userNew.setAge(24);
            return d.document(userNew);
        }).build();

        BulkOperation build3 = new Builder().create(d -> {
            UserNew userNew = new UserNew();
            userNew.setSex("男");
            userNew.setName("zhangsan");
            userNew.setAge(26);
            return d.document(userNew);
        }).build();

        operationArrayList.add(build1);
        operationArrayList.add(build2);
        operationArrayList.add(build3);


        client.bulk(fn -> {
            return fn.index("user_new").operations(operationArrayList);
        });

    }

    /**
     * update document
     * @author Leo
     * @param client client
     */
    public static void updateDocById(ElasticsearchClient client) throws IOException {
        UserNew userNew = new UserNew();
        userNew.setAge(25);
        client.update(fn -> {
            return fn.index("user_new").id("1001").doc(userNew);
        }, userNew.getClass());
    }

    /**
     * search document by id
     * @author Leo
     * @param client client
     */
    public static void getDocById(ElasticsearchClient client) throws IOException {
        GetResponse<UserNew> response = client.get(fn ->
                fn.index("user_new").id("1001")
            , UserNew.class);
        log.info("result:{}", response.source());
    }

    /**
     * remove es document by id
     * @param client client
     */
    public static void removeDocById(ElasticsearchClient client) throws IOException {
        DeleteResponse response = client.delete(fn -> {
            return fn.index("user_new").id("1002");
        });
        log.info("result:{}", response.result());
    }




}
