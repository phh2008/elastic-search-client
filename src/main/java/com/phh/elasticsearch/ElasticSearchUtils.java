package com.phh.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 * 工具类
 * </p>
 *
 * @author phh
 * @version V1.0
 * @project: ufa_b2
 * @package com.ufa.elastic.search.util
 * @date 2018/1/20
 */
public class ElasticSearchUtils {

    /**
     * <p>
     * 查询响应SearchResponse
     * 提取hits并获取source 转换为map集合
     * </p>
     *
     * @param response 查询响应
     * @return list->map
     * @author phh
     * @date 2018/1/20
     * @version V1.0
     */
    public final static List<Map<String, Object>> getHitSourceAsMap(SearchResponse response) {
        return getHitSourceAsMap(response, null, false);
    }

    public final static List<Map<String, Object>> getHitSourceAsMap(SearchResponse response, Function<SearchHit, Map<String, Object>> hitFunc) {
        return getHitSourceAsMap(response, hitFunc, false);
    }

    /**
     * <p>
     * 查询响应SearchResponse
     * 提取hits并获取source 转换为map集合
     * </p>
     *
     * @param response  查询响应
     * @param highlight 是否返回设置高亮的字段
     * @return list->map
     * @throws
     * @author phh
     * @date 2018/1/22
     * @version V1.0
     */
    public final static List<Map<String, Object>> getHitSourceAsMap(SearchResponse response, Function<SearchHit, Map<String, Object>> hitFunc, boolean highlight) {
        return StreamSupport.stream(response.getHits().spliterator(), false)
                .map(hit -> {
                    Map<String, Object> map;
                    if (hitFunc != null) {
                        map = hitFunc.apply(hit);
                    } else {
                        map = hit.getSourceAsMap();
                    }
                    if (highlight) {
                        Map<String, HighlightField> highMap = hit.getHighlightFields();
                        if (highMap != null && highMap.size() > 0) {
                            highMap.values().stream().forEach(hlField -> {
                                Text[] frags = hlField.getFragments();
                                map.put(hlField.getName().concat("HL"), frags != null && frags.length > 0 ? Objects.toString(frags[0], "") : "");
                            });
                        }
                    }
                    return map;
                })
                .collect(Collectors.toList());
    }

    public final static List<Map<String, Object>> getHitSourceAsMap(SearchResponse response, boolean highlight) {
        return getHitSourceAsMap(response, null, highlight);
    }

}