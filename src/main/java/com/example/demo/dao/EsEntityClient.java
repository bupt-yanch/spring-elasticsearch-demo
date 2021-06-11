package com.example.demo.dao;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.entity.Entity;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Repository
@Log4j2
public class EsEntityClient {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 设置索引名
    private static final String INDEX_NAME = "entity";

    public Entity queryEntityById(String id) {
        GetRequest getRequest = new GetRequest(INDEX_NAME).id(id);

        Entity entity = null;
        try {
            GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            entity = JSONObject.parseObject(JSONObject.toJSONString(response.getSource()), Entity.class);
        } catch (IOException e) {
            log.warn("can't find entity, id:{}", id, e);
        }
        return entity;
    }

    public List<Entity> queryByIds(List<String> ids) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        idsQueryBuilder.ids().addAll(ids);

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(idsQueryBuilder);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder)
                // 这里可以设置多索引
                .indices(INDEX_NAME);

        List<Entity> entities = new ArrayList<>();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            // 根据score倒序排序(相关度排序)
            Arrays.sort(hits, (h1, h2) -> (int) (h2.getScore() - h1.getScore()));
            for (SearchHit hit : hits) {
                String jsonString = hit.getSourceAsString();
                entities.add(JSONObject.parseObject(jsonString, Entity.class));
            }
        } catch (IOException e) {
            log.warn("search by ids failed. ids:{}", ids.toString(), e);
        }
        return entities;
    }

    public List<Entity> queryEntityByName(String name) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();

        // 使用termQuery，第一个参数为：目标字段名.keyword，就可以实现对这个参数的精准匹配
        queryBuilder.filter(QueryBuilders.termQuery("name" + ".keyword", name));
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(queryBuilder).size(20);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(INDEX_NAME);

        List<Entity> entities = new ArrayList<>();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            Arrays.sort(hits, (h1, h2) -> (int) (h2.getScore() - h1.getScore()));
            for (SearchHit hit : hits) {
                String jsonString = hit.getSourceAsString();
                entities.add(JSONObject.parseObject(jsonString, Entity.class));
            }
        } catch (IOException e) {
            log.warn("search entities failed. name:{}", name, e);
        }
        return entities;
    }

    public List<Entity> query(String name, String summary, String introduction) {
        BoolQueryBuilder queryBuilder = buildFuzzQueryBuilder(name, summary, introduction);
        // 暂时写死查100个
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(queryBuilder).size(100);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
        // 设置查询范围
        searchRequest.indices(INDEX_NAME);

        List<Entity> entities = new ArrayList<>();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            Arrays.sort(hits, (h1, h2) -> (int) (h2.getScore() - h1.getScore()));
            for (SearchHit hit : hits) {
                String jsonString = hit.getSourceAsString();
                entities.add(JSONObject.parseObject(jsonString, Entity.class));
            }
        } catch (IOException e) {
            log.warn("search failed.", e);
        }
        return entities;
    }

    // 构建查询
    private BoolQueryBuilder buildFuzzQueryBuilder(String name, String summary, String introduction) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        if (Strings.isNotEmpty(name)) {
            // 模糊匹配
            MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("name", name);
            boolQueryBuilder.filter(queryBuilder);
        }

        if (Strings.isNotEmpty(summary)) {
            // 模糊匹配
            MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("summary", summary);
            boolQueryBuilder.filter(queryBuilder);
        }

        if (Strings.isNotEmpty(introduction)) {
            // 模糊匹配
            MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("introduction", introduction);
            boolQueryBuilder.filter(queryBuilder);
        }
        return boolQueryBuilder;
    }
}
