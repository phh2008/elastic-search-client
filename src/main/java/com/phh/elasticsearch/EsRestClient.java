package com.phh.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * <p>
 * elasticSearch rest api client
 * </p>
 *
 * @author phh
 * @version V1.0
 */
public class EsRestClient extends RestHighLevelClient {

    private static final Logger logger = LoggerFactory.getLogger(EsRestClient.class);

    /**
     * <p>
     * elasticSearch地址列表
     * e.g. http://127.0.0.1:9200,http://127.0.0.1:9201
     * 多个地址用豆号分隔开
     * </p>
     *
     * @param
     */
    public EsRestClient(String httpHosts) {
        //checkArgument(!Strings.isNullOrEmpty(httpHosts), "http host is null or empty")
        super(RestClient.builder(Arrays.stream(httpHosts.split(","))
                .map(e -> HttpHost.create(e))
                .toArray(HttpHost[]::new)));
    }


    /**
     * <p>
     * 滑动查询
     * (深度分页查询)
     * </p>
     *
     * @param index            索引
     * @param scrollId         每次滑动查询返回的id标识
     * @param keepAlive        scroll查询内部缓存每次查询快照并设置的有效期
     * @param searchSrcBuilder 查询请求构建
     * @return SearchResponse
     * @throws Exception if error
     */
    public SearchResponse searchScroll(String index, String scrollId, TimeValue keepAlive, SearchSourceBuilder searchSrcBuilder) throws Exception {
        SearchRequest req = new SearchRequest(index);
        req.scroll(keepAlive);
        req.source(searchSrcBuilder);
        logger.info("elasticsearch dsl :" + req.toString());
        if (Strings.isNullOrEmpty(scrollId)) {
            return this.search(req);
        } else {
            SearchScrollRequest scrollRequest = new SearchScrollRequest();
            scrollRequest.scrollId(scrollId);
            scrollRequest.scroll(keepAlive);
            try {
                return this.searchScroll(scrollRequest);
            } catch (ElasticsearchStatusException e) {
                logger.error("searchScroll error", e);
                if (RestStatus.NOT_FOUND == e.status()) {
                    //可能scrollId过期了，attempt search
                    //TODO 或还有其它方式，可提示调用端
                    return this.search(req);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * <p>
     * 滑动查询
     * </p>
     *
     * @param index        索引
     * @param scrollId     每次滑动查询返回的id标识
     * @param keepAlive    scroll查询内部缓存每次查询快照并设置的有效期
     * @param size         每查询返回的条数(如果有分片则返回的是size*分片数)
     * @param queryBuilder 查询构建器
     * @return SearchResponse
     * @throws Exception if error
     */
    public SearchResponse searchScroll(String index, String scrollId, TimeValue keepAlive, int size, QueryBuilder queryBuilder) throws Exception {
        SearchSourceBuilder searchSrcBuilder = new SearchSourceBuilder();
        searchSrcBuilder.size(size)
                .query(queryBuilder);
        return searchScroll(index, scrollId, keepAlive, searchSrcBuilder);
    }


    /**
     * <p>
     * 分页查询
     * </p>
     *
     * @param index        索引
     * @param pageNo       当前页数
     * @param pageSize     每页数量
     * @param queryBuilder 查询构建器
     * @return SearchResponse
     * @throws Exception if error
     */
    public SearchResponse searchPage(String index, int pageNo, int pageSize, QueryBuilder queryBuilder) throws Exception {
        SearchSourceBuilder searchSrcBuilder = new SearchSourceBuilder();
        searchSrcBuilder.from((pageNo - 1) * pageSize);
        searchSrcBuilder.size(pageSize);
        searchSrcBuilder.query(queryBuilder);
        // 设置是否按查询匹配度排序 _score
        searchSrcBuilder.explain(true);
        SearchRequest req = new SearchRequest(index);
        req.source(searchSrcBuilder);
        logger.info("elasticsearch dsl :" + req.toString());
        return this.search(req);
    }

    public SearchResponse searchPage(String index, int pageNo, int pageSize, QueryBuilder queryBuilder, SortBuilder<?> sortBuilder) throws Exception {
        SearchSourceBuilder searchSrcBuilder = new SearchSourceBuilder();
        searchSrcBuilder.from((pageNo - 1) * pageSize);
        searchSrcBuilder.size(pageSize);
        searchSrcBuilder.query(queryBuilder);
        if (sortBuilder != null) {
            searchSrcBuilder.sort(sortBuilder);
        } else {
            // 设置是否按查询匹配度排序 _score
            searchSrcBuilder.explain(true);
        }
        SearchRequest req = new SearchRequest(index);
        req.source(searchSrcBuilder);
        logger.info("elasticsearch dsl :" + req.toString());
        return this.search(req);
    }

}
