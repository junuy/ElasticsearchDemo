package com.junuy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;
import com.junuy.domain.UserInfo;

/**
 * 文档操作
 *
 * @author junuy 2021/3/8
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryTest {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    public static final String ES_INDEX = "test_index";

    // 创建文档
    @Test
    public void createDocument() throws IOException {
        // 创建对象
        UserInfo userInfo = new UserInfo("张三", 12, "法外狂徒");
        // 创建请求
        IndexRequest request = new IndexRequest(ES_INDEX);
        // 规则
        request.id("1").timeout(TimeValue.timeValueSeconds(1));
        // 将数据放到请求中
        request.source(JSON.toJSONString(userInfo), XContentType.JSON);
        // 客户端发送请求，获取相应的结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        // 打印一下
        System.out.println(response.toString());
        System.out.println(response.status());
    }

    // IndexResponse[index=test_index,type=_doc,id=1,version=1,result=created,seqNo=0,primaryTerm=1,shards={"total":2,"successful":1,"failed":0}]
    // CREATED

    // 判断是否存在
    @Test
    public void exitDocument() throws IOException {
        GetRequest request = new GetRequest(ES_INDEX, "1");
        // 不获取返回的_source 的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none");

        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // true

    // 获取文档信息
    @Test
    public void getDocument() throws IOException {
        GetRequest request = new GetRequest(ES_INDEX, "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("获取到的结果" + response.getSourceAsString());
    }

    // 获取到的结果{"age":12,"desc":"法外狂徒","name":"张三"}

    // 更新文档
    @Test
    public void updateDocument() throws IOException {
        // 创建对象
        UserInfo userInfo = new UserInfo("张三", 12, "法外新狂徒");

        UpdateRequest request = new UpdateRequest(ES_INDEX, "1");
        request.timeout("1s");

        request.doc(JSON.toJSONString(userInfo), XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    // OK

    // 删除文档
    @Test
    public void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest(ES_INDEX, "1");
        request.timeout("1s");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    // OK

    // 批量添加
    @Test
    public void bulkDocument() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");

        ArrayList<UserInfo> userInfos = new ArrayList<>();
        userInfos.add(new UserInfo("张三", 1, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 2, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 3, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 4, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 5, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 6, "法外狂徒"));
        userInfos.add(new UserInfo("张三", 7, "法外狂徒"));

        // 进行批处理请求
        for (int i = 0; i < userInfos.size(); i++) {
            request.add(new IndexRequest(ES_INDEX).id("" + (i + 1)).source(JSON.toJSONString(userInfos.get(i)),
                XContentType.JSON));
        }

        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(response.hasFailures());
    }

    // false

    // 查询
    @Test
    public void SearchDocument() throws IOException {
        SearchRequest request = new SearchRequest(ES_INDEX);
        // 构建搜索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();

        // 查询条件使用QueryBuilders工具来实现
        // QueryBuilders.termQuery 精准查询
        // QueryBuilders.matchAllQuery() 匹配全部
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "张三");
        builder.query(matchQuery);
        builder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("查询出的结果" + JSON.toJSONString(response.getHits()));
    }

    // 查询出的结果{"fragment":true,"hits":[{"fields":{},"fragment":false,"highlightFields":{},"id":"1","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":1,"desc":"法外狂徒"},"sourceAsString":"{\"age\":1,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"2","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":2,"desc":"法外狂徒"},"sourceAsString":"{\"age\":2,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"3","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":3,"desc":"法外狂徒"},"sourceAsString":"{\"age\":3,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"4","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":4,"desc":"法外狂徒"},"sourceAsString":"{\"age\":4,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"5","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":5,"desc":"法外狂徒"},"sourceAsString":"{\"age\":5,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"6","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":6,"desc":"法外狂徒"},"sourceAsString":"{\"age\":6,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1},{"fields":{},"fragment":false,"highlightFields":{},"id":"7","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":0.10258658,"seqNo":-2,"sortValues":[],"sourceAsMap":{"name":"张三","age":7,"desc":"法外狂徒"},"sourceAsString":"{\"age\":7,\"desc\":\"法外狂徒\",\"name\":\"张三\"}","sourceRef":{"fragment":true},"type":"_doc","version":-1}],"maxScore":0.10258658,"totalHits":{"relation":"EQUAL_TO","value":7}}
}
