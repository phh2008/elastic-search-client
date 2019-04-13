package com.phh.elasticsearch.test;

import com.phh.elasticsearch.EsRestClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

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

    private EsRestClient client = new EsRestClient("http://120.78.85.72:99");

    @Test
    public void testCreateIndex() throws IOException {
    }

}
