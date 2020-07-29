package com.phh.elasticsearch.test;

import com.alibaba.fastjson.JSON;
import com.phh.elasticsearch.EsRestClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p> TODO
 *
 * @author phh
 * @version V1.0
 * @project: spring
 * @package com.phh.elasticsearch.test
 * @date 2019/2/28
 */
@Slf4j
public class DocTest {

    private EsRestClient client = new EsRestClient("http://127.0.0.1:9200");


    /**
     * 添加文档
     *
     * @throws IOException
     */
    @Test
    public void testAdd() throws IOException {
        IndexRequest indexRequest = new IndexRequest("test_doc");
        Map<String, Object> source = new HashMap<>();
        source.put("id", 1000);
        source.put("title", "中国农业银行东莞南城分行");
        source.put("test_not", "这是一个不存在的字段");
        indexRequest.source(source);
        indexRequest.id(source.get("id").toString());
        IndexResponse res = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info(res.toString());
    }

    @Data
    class Bank {
        private Long id;
        private String title;
    }

    @Test
    public void testAdd2() throws IOException {
        IndexRequest indexRequest = new IndexRequest("test_doc");
        Bank bank = new Bank();
        bank.setId(1000L);
        bank.setTitle("测试银行");
        indexRequest.source(JSON.toJSONString(bank), XContentType.JSON);
        indexRequest.id(bank.getId().toString());
        IndexResponse res = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info(res.toString());
        log.info(res.status().getStatus() + "");
        log.info("{}", RestStatus.OK == res.status());
    }

    @Test
    public void testAddBatch() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("test_doc");
        Map<String, Object> source = new HashMap<>();
        source.put("id", 1001);
        source.put("title", "中国农业银株洲支行");
        indexRequest.source(source);
        indexRequest.id(source.get("id").toString());

        IndexRequest indexRequest2 = new IndexRequest("test_doc");
        Map<String, Object> source2 = new HashMap<>();
        source2.put("id", 1002);
        source2.put("title", "中国农业银长沙支行");
        indexRequest2.source(source2);
        indexRequest2.id(source2.get("id").toString());

        bulkRequest.add(indexRequest);
        bulkRequest.add(indexRequest2);
        BulkResponse res = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info(res.status().toString());
    }

    /**
     * 删除文档
     *
     * @throws IOException
     */
    @Test
    public void testDel() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test_doc");
        deleteRequest.id("paz3NGkBWmij0rZJtQyZ");
        DeleteResponse res = client.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info(res.status().toString());
    }

    /**
     * 更新文档
     *
     * @throws IOException
     */
    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test_doc", "1002");
        Map<String, Object> map = new HashMap<>();
        map.put("id", 1002);
        map.put("title", "中国农业银长沙岳麓区支行");
        updateRequest.doc(map);
        UpdateResponse res = client.update(updateRequest, RequestOptions.DEFAULT);
        log.info(res.status().toString());
    }

    @Test
    public void testDateGeoType() throws IOException {
        String index_mapping = "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":2},\"mappings\":{\"doc\":{\"properties\":{\"id\":{\"type\":\"long\",\"store\":true},\"title\":{\"type\":\"text\",\"store\":true,\"analyzer\":\"ik_max_word\",\"search_analyzer\":\"ik_max_word\"},\"createTime\":{\"type\":\"date\",\"store\":true,\"format\":\"yyyy-MM-dd HH:mm:ss\"},\"location\":{\"type\":\"geo_point\",\"store\":true}}}}}";

        IndexRequest indexRequest = new IndexRequest("test_doc_geo");
        Map<String, Object> source = new HashMap<>();
        source.put("id", 1003);
        source.put("title", "中国农业银东莞分行");
        source.put("createTime", "2019-03-01 17:33:00");
        source.put("location", new HashMap<String, Double>() {{
            put("lat", 22.9348461312);
            put("lon", 113.6919602600);
        }});
        //source.put("location", new double[]{-69.34, 39.12});
        //地理位置，其值可以有如下四中表现形式：
        //object对象："location": {"lat": 41.12, "lon": -71.34}
        //字符串："location": "41.12,-71.34"
        //geohash："location": "drm3btev3e86"
        //数组："location": [ -71.34, 41.12 ]

        indexRequest.source(source);
        indexRequest.id(source.get("id").toString());
        IndexResponse res = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info(res.toString());

    }

}
