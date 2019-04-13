package com.phh.elasticsearch.test;

import com.alibaba.fastjson.JSON;
import com.phh.elasticsearch.ElasticSearchUtils;
import com.phh.elasticsearch.EsRestClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
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
public class QueryTest {

    private EsRestClient client = new EsRestClient("http://120.78.85.72:99");

    /**
     * scroll查询
     *
     * @throws Exception
     */
    @Test
    public void testScrollQuery() throws Exception {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.size(3)
                .highlighter(new HighlightBuilder().field("branchName").preTags("<em>").postTags("</em>"))
                .query(QueryBuilders.multiMatchQuery("农业银行", "branchName", "branchName.py")
                        .type(MultiMatchQueryBuilder.Type.MOST_FIELDS));
        SearchResponse resp = client.searchScroll("bank", null, TimeValue.timeValueMinutes(5), searchBuilder);
        List<Map<String, Object>> list = ElasticSearchUtils.getHitSourceAsMap(resp, true);
        log.info("scrollId:{}", resp.getScrollId());
        log.info(JSON.toJSONString(list, true));
    }

    /**
     * 获取所有记录
     *
     * @throws Exception
     */
    @Test
    public void testQueryAll() throws Exception {
        int pageNo = 1;
        int pageSize = 2;
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, QueryBuilders.matchAllQuery());
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * 多字段查询
     *
     * @throws Exception
     */
    @Test
    public void testMultiMatchQuery() throws Exception {
        int pageNo = 1;
        int pageSize = 5;
        String keywords = "农业银行";
        //多字段查询
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders
                .multiMatchQuery(keywords, new String[]{"branchName", "code"})
                .field("branchName", 1.5F)
                .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).tieBreaker(0.3F);
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, multiMatchQueryBuilder);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * 精确查询
     */
    @Test
    public void testTermQuery() throws Exception {
        int pageNo = 1;
        int pageSize = 5;
        TermQueryBuilder query = QueryBuilders.termQuery("code", "103233077427");
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, query);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * bool联合查询
     */
    @Test
    public void testBoolQuery() throws Exception {
        int pageNo = 1;
        int pageSize = 5;
        String keywords = "农业银行";
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchQuery("branchName", keywords))
                .filter(QueryBuilders.termQuery("code", "103233077427"));
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, boolQueryBuilder);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * 排序，指定字段排序(会使默认_score排序失效)
     */
    @Test
    public void testSort() throws Exception {
        int pageNo = 1;
        int pageSize = 5;
        String keywords = "农业银行";
        MatchQueryBuilder query = QueryBuilders.matchQuery("branchName", keywords);
        SortBuilder<?> sortBuilder = SortBuilders.fieldSort("bankId").order(SortOrder.DESC);
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, query, sortBuilder);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * 排序:结合默认的_score得分+额外分数
     */
    @Test
    public void testSort2() throws Exception {
        int pageNo = 1;
        int pageSize = 5;
        String keywords = "农业银行";
        MatchQueryBuilder query = QueryBuilders.matchQuery("branchName", keywords);
        FieldValueFactorFunctionBuilder fieldQuery = ScoreFunctionBuilders
                .fieldValueFactorFunction("bankId")
                .modifier(FieldValueFactorFunction.Modifier.LOG1P)
                .factor(0.1f);
        // 最终分数=_score+额外分数
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders
                .functionScoreQuery(query, fieldQuery)
                .boostMode(CombineFunction.SUM);
        SearchResponse res = client.searchPage("bank", pageNo, pageSize, functionScoreQueryBuilder);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res), true));
    }

    /**
     * geo地理位置查询
     */
    @Test
    public void testGeoQuery() throws Exception {
        double lat = 22.9484666581;
        double lon = 113.6791411511;
        double distance = 3000d;
        //查询附近的数据
        GeoDistanceQueryBuilder builder =
                QueryBuilders.geoDistanceQuery("location")//查询字段
                        .point(lat, lon)//设置经纬度
                        .distance(distance, DistanceUnit.METERS)//设置距离和单位（米）
                        .geoDistance(GeoDistance.ARC);
        //距离排序
        GeoDistanceSortBuilder sortBuilder =
                SortBuilders.geoDistanceSort("location", lat, lon)
                        .unit(DistanceUnit.METERS)
                        .order(SortOrder.ASC);//排序方式
        SearchResponse res = client.searchPage("test_doc_geo", 1, 5, builder, sortBuilder);
        log.info("total:{}", res.getHits().getTotalHits());
        log.info(JSON.toJSONString(ElasticSearchUtils.getHitSourceAsMap(res, (hit -> {
            Map<String, Object> map = hit.getSourceAsMap();
            //提取距离值
            map.put("distance", new BigDecimal((Double) hit.getSortValues()[0]).setScale(2, BigDecimal.ROUND_HALF_DOWN));
            return map;
        })), true));
    }


}
