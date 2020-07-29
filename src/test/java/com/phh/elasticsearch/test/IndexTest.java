package com.phh.elasticsearch.test;

import com.phh.elasticsearch.EsRestClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;

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
public class IndexTest {

    private EsRestClient client = new EsRestClient("http://127.0.0.1:9200");

    /**
     * 创建索引
     *
     * @throws IOException
     */
    @Test
    public void testCreateIndex() throws IOException {
        String indexName = "test_doc";
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("properties")
                .startObject()
                .field("id").startObject().field("store", true).field("index", true).field("type", "long").endObject()
                .field("title").startObject().field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").field("store", true).field("index", true).field("type", "text").endObject()
                .endObject()
                .endObject();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping(builder);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("isAcknowledged::" + acknowledged);
    }

    /**
     * 删除索引
     */
    @Test
    public void testDeleteIndex() throws IOException {
        String indexName = "test_doc";
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse res = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("isAcknowledged::" + res.isAcknowledged());
    }

}
