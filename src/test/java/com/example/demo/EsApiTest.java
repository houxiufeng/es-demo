package com.example.demo;

import com.example.demo.common.JacksonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//@SpringBootTest(classes = DemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EsApiTest {

    private ElasticsearchRestTemplate esRestTemplate;
    private RestHighLevelClient elasticsearchClient;

    @BeforeEach
    public void init() {
        elasticsearchClient =  new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.123.184", 9200, "http")));
        esRestTemplate = new ElasticsearchRestTemplate(elasticsearchClient);
    }

    @AfterEach
    public void destroy() throws IOException {
        elasticsearchClient.close();
    }

    // 测试添加文档
    @Test
    public void testAddDocument() throws IOException {
        // 创建一个doc
        Map<String, Object> doc = Maps.newHashMap();
        doc.put("name", "allen");
        doc.put("age", 22);
        doc.put("desc", "hello world");
        doc.put("tags", Lists.newArrayList("apple", "banana", "pear"));
        // 创建请求
        IndexRequest request = new IndexRequest("alice");
        // 制定规则 PUT /alice/_doc/xxxx
        request.id(UUID.randomUUID().toString());// 设置文档ID
        request.timeout(TimeValue.timeValueMillis(1000));// request.timeout("1s")
        // 将我们的数据放入请求中
        request.source(JacksonUtil.bean2Json(doc), XContentType.JSON);
        // 客户端发送请求，获取响应的结果
        IndexResponse response = elasticsearchClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.status());// 获取建立索引的状态信息 CREATED
        System.out.println(response);// 查看返回内容 IndexResponse[index=alice,type=_doc,id=d831a99c-282d-413a-8703-f1b8b3735236,version=1,result=created,seqNo=24,primaryTerm=2,shards={"total":1,"successful":1,"failed":0}]
    }

    // 测试获得文档信息
    @Test
    public void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("alice","1");
        GetResponse response = elasticsearchClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());// 打印文档内容
        System.out.println(request);// 返回的全部内容和命令是一样的
    }

    // 获取文档，判断是否存在 get /alice/_doc/1
    @Test
    public void testDocumentIsExists() throws IOException {
        GetRequest request = new GetRequest("alice", "1");
        // 不获取返回的 _source的上下文了
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists = elasticsearchClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 测试更新文档内容(可以部分更新)
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("alice", "1");
        Map<String, Object> doc = Maps.newHashMap();
        doc.put("name", "allen111");
        doc.put("age", 22);
        request.doc(JacksonUtil.bean2Json(doc), XContentType.JSON);
        UpdateResponse response = elasticsearchClient.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status()); // OK
    }

    // 测试删除文档
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("alice", "d831a99c-282d-413a-8703-f1b8b3735236");
        request.timeout("1s");
        DeleteResponse response = elasticsearchClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());// OK
    }

    /**
     * 一次性取多条id
     * @throws IOException
     */
    @Test
    public void test2() throws IOException {
        String[] includes = new String[]{"name", "age"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add(new MultiGetRequest.Item("alice",null,"1").fetchSourceContext(fetchSourceContext)); //每个item还可以设置返回的source
        multiGetRequest.add("alice",null,"2");  //直接添加查询条件
        MultiGetResponse multiGetItemResponses = elasticsearchClient.multiGet(multiGetRequest, RequestOptions.DEFAULT);
        MultiGetItemResponse[] responses = multiGetItemResponses.getResponses();
        for(MultiGetItemResponse response : responses){
            System.out.println(response.getResponse().getSource().keySet());
        }
    }



    /**
     * GET alice/_count
     * IndexCoordinates.of(xxx)  根据官方注释 可以知道这个类是设置索引名称和类型的。
     */
    @Test
    public void testCount() throws Exception {
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        long count = esRestTemplate.count(searchQueryBuilder.build(), IndexCoordinates.of("alice"));
        System.out.println("count ->" + count);
    }

    /**
     * GET alice/_count
     * {
     *   "query": {
     *       "query_string": {
     *           "query": "爱",
     *           "fields": [
     *               "name^1.0"
     *           ]
     *       }
     *   }
     * }
     * ref: https://blog.csdn.net/csdn_20150804/article/details/105618933
     */
    @Test
    public void testCount2() throws Exception {
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        searchQueryBuilder.withQuery(QueryBuilders.queryStringQuery("爱").field("name"));
        long count = esRestTemplate.count(searchQueryBuilder.build(), IndexCoordinates.of("alice"));
        System.out.println("count ->" + count);
    }

    /**
     * GET alice/_count
     * {
     *     "query": {
     *         "bool": {
     *             "must": [
     *                 {
     *                   "term": {
     *                       "name": "爱"
     *                   }
     *                 }
     *             ]
     *         }
     *     }
     * }
     *  term 也可以用 match 替换
     */
    @Test
    public void testCount3() throws Exception {
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        searchQueryBuilder.withQuery(boolQueryBuilder.must(QueryBuilders.termQuery("name","爱")));
        searchQueryBuilder.withQuery(boolQueryBuilder.must(QueryBuilders.matchQuery("name","爱")));
        long count = esRestTemplate.count(searchQueryBuilder.build(), IndexCoordinates.of("alice"));
        System.out.println("count ->" + count);
    }


    /**
     * GET alice/_search
     * {
     *   "query": {
     *     "bool" : {
     *       "must" : [
     *         {
     *           "term" : {
     *             "name" : {
     *               "value" : "爱"
     *             }
     *           }
     *         },
     *         {
     *           "range" : {
     *             "age" : {
     *               "from" : 23,
     *               "to" : 26
     *             }
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     * term 也可以用 match 替换
     * @throws Exception
     */
    @Test
    public void testBooleanQuery() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // 查询name=爱
//        boolQueryBuilder.must().add(new TermQueryBuilder("name", "爱"));
        boolQueryBuilder.must().add(new MatchQueryBuilder("name", "爱"));

        // 查询[23-26]之间的数据
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gte(23).lte(26);
        boolQueryBuilder.must().add(rangeQueryBuilder);

        // 分页查询20条
        PageRequest pageRequest = PageRequest.of(0, 20, Sort.by("age").descending());
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        searchQueryBuilder.withQuery(boolQueryBuilder).withPageable(pageRequest);

        Query searchQuery = searchQueryBuilder.build();
        SearchHits<Map> hits = esRestTemplate.search(searchQuery, Map.class, IndexCoordinates.of("alice"));
        List<SearchHit<Map>> hitList = hits.getSearchHits();
        System.out.println("hit size -> " + hitList.size());
        hitList.forEach(hit -> {
            System.out.println("返回数据：" + hit.getContent());
        });
    }

    /**
     * GET alice/_search
     * {
     *   "query": {
     *     "bool": {
     *         "must": [
     *             {
     *                 "match": {
     *                     "name": {
     *                         "query": "爱"
     *                     }
     *                 }
     *             }
     *         ],
     *         "filter": [
     *             {
     *                 "range": {
     *                     "age": {
     *                         "from": 23,
     *                         "to": 26
     *                     }
     *                 }
     *             }
     *         ]
     *     }
     *   }
     * }
     * @throws Exception
     */
    @Test
    public void testBooleanQuery2() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // 查询name=爱
        boolQueryBuilder.must().add(new MatchQueryBuilder("name", "爱"));

        // 查询[23-26]之间的数据
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gte(23).lte(26);
        boolQueryBuilder.filter().add(rangeQueryBuilder);

        // 分页查询20条
        PageRequest pageRequest = PageRequest.of(0, 20, Sort.by("age").descending());
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        searchQueryBuilder.withQuery(boolQueryBuilder).withPageable(pageRequest);

        Query searchQuery = searchQueryBuilder.build();
        SearchHits<Map> hits = esRestTemplate.search(searchQuery, Map.class, IndexCoordinates.of("alice"));
        List<SearchHit<Map>> hitList = hits.getSearchHits();
        System.out.println("hit size -> " + hitList.size());
        hitList.forEach(hit -> {
            System.out.println("返回数据：" + hit.getContent());
        });
    }


    /**
     * bulk insert
     * @throws IOException
     */
    @Test
    public void testBulkInsert() throws IOException {
        Map<String, Object> doc1 = Maps.newHashMap();
        doc1.put("name","xx1");
        doc1.put("age",1111);
        Map<String, Object> doc2 = Maps.newHashMap();
        doc2.put("name","xx2");
        doc2.put("age",1222);
        Map<String, Object> doc3 = Maps.newHashMap();
        doc3.put("name","xx3");
        doc3.put("age",333);
        IndexRequest indexRequest1 = new IndexRequest("alice").id(UUID.randomUUID().toString()).source(doc1);
        IndexRequest indexRequest2 = new IndexRequest("alice").id(UUID.randomUUID().toString()).source(doc2);
        IndexRequest indexRequest3 = new IndexRequest("alice").id(UUID.randomUUID().toString()).source(doc3);
        BulkRequest request = new BulkRequest();
        request.add(indexRequest1);
        request.add(indexRequest2);
        request.add(indexRequest3);
        BulkResponse bulkItemResponses = elasticsearchClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulkItemResponses.getItems().length);
    }

    /**
     * GET alice/_search
     * {
     *   "query": {
     *     "bool" : {
     *       "must" : [
     *         {
     *           "multi_match" : {
     *             "query" : "java",
     *             "fields" : [
     *               "desc",
     *               "name"
     *             ]
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     * @throws Exception
     */
    @Test
    public void testMultiMatch() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // 查询name=爱
        boolQueryBuilder.must().add(QueryBuilders.multiMatchQuery("java", "name", "desc"));

//        // 查询[23-26]之间的数据
//        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
//        rangeQueryBuilder.gte(23).lte(26);
//        boolQueryBuilder.filter().add(rangeQueryBuilder);

        // 分页查询20条
        PageRequest pageRequest = PageRequest.of(0, 20, Sort.by("age").descending());
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        searchQueryBuilder.withQuery(boolQueryBuilder).withPageable(pageRequest);

        Query searchQuery = searchQueryBuilder.build();
        SearchHits<Map> hits = esRestTemplate.search(searchQuery, Map.class, IndexCoordinates.of("alice"));
        List<SearchHit<Map>> hitList = hits.getSearchHits();
        System.out.println("hit size -> " + hitList.size());
        hitList.forEach(hit -> {
            System.out.println("返回数据：" + hit.getContent());
        });
    }
}
