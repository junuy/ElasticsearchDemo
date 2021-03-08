package com.junuy;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * 代码描述
 * 
 * @author junuy 2021/3/8
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexTest {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // index名字，静态一般都是放在另一个类中的
    public static final String ES_INDEX = "test_index";

    // 创建索引
    @Test
    public void createIndex() throws IOException {
        // 1. 创建索引
        CreateIndexRequest index = new CreateIndexRequest(ES_INDEX);
        // 2. 客户端执行请求,请求后获得相应
        CreateIndexResponse response = client.indices().create(index, RequestOptions.DEFAULT);
        // 3.打印结果
        System.out.println(response.toString());
    }

    // 测试索引是否存在
    @Test
    public void exitIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest(ES_INDEX);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("是否存在" + exists);
    }

    // 删除索引
    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(ES_INDEX);
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("是否删除" + response);
    }
}
