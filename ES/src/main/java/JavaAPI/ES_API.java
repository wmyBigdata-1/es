package JavaAPI;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * java API 操作ES
 */
public class ES_API {
    private TransportClient client;

    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws Exception {

        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.22.114"), 9300));
    }

    /**
     * 创建索引
     */
    @Test
    public void createIndex_blog(){
        //创建索引
        client.admin().indices().prepareCreate("blog1").get();

        //关闭连接
        client.close();
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex_blog(){
        //删除索引
        client.admin().indices().prepareDelete("blog");

        //关闭连接
        client.close();
    }

    /**
     * 数据源是json格式的
     */
    @Test
    public void createIndexByJson() {
        // 1 文档数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于createIndexByJson的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        //2、创建文档
        //prepareUpdate(String index, String type, String id)
        /**
         * Sets the document source to index.
         * <p>
         * Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
         * or using the {@link #setSource(byte[], XContentType)}.
         */
        IndexResponse indexResponse = client.prepareIndex("blog", "artitle", "1")
                .setSource(json, XContentType.JSON).execute().actionGet();

        //打印返回的结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("version: " + indexResponse.getVersion());

        //关闭连接
        client.close();
    }

    /**
     * 新建文档 源数据map方式添加json
     */
    @Test
    public void createIndexByMap(){
        // 1 文档数据准备
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        IndexResponse indexResponse = client.prepareIndex("blog", "artitle", "1")
                .setSource(json, XContentType.JSON).execute().actionGet();

        //打印返回的结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("version: " + indexResponse.getVersion());

        //关闭连接
        client.close();

    }

    /**
     * 新建文档（源数据ES构建起添加Json格式）
     */
    @Test
    public void createIndex() throws Exception {

        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject().field("id", 3).field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "artitle", "3").setSource(builder).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    /**
     * 检索全文
     */
    @Test
    public void getData() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog", "artitle", "1").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        // 3 关闭连接
        client.close();
    }

    @Test
    public void getMultiData() {

        // 1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet().add("blog", "artitle", "1","2","3").get();

        // 2 遍历返回的结果
        for(MultiGetItemResponse itemResponse:response){
            GetResponse getResponse = itemResponse.getResponse();

            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        // 3 关闭资源
        client.close();
    }

    @Test
    public void updateData() throws Throwable {

        // 1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("artitle");
        updateRequest.id("3");


        /**
         * Sets the doc to use for updates when a script is not specified.
         */
        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器")
                .field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .field("createDate", "2020-5-25").endObject());

        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    @Test
    public void testUpsert() throws Exception {

        // 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blog", "artitle", "4")
                .source(XContentFactory.jsonBuilder().startObject().field("title", "搜索服务器").field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。").endObject());

        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blog", "artitle", "4")
                .doc(XContentFactory.jsonBuilder().startObject().field("user", "李四").endObject()).upsert(indexRequest);

        client.update(upsert).get();
        client.close();
    }

    @Test
    public void deleteData() {

        // 1 删除文档数据
        DeleteResponse indexResponse = client.prepareDelete("blog", "artitle", "1").get();

        // 3 关闭连接
        client.close();
    }

    /**
     * 条件查询
     */
    @Test
    public void matchAllQuery() {

        // 1 执行查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("artitle")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());//打印出每条结果
        }

        // 3 关闭连接
        client.close();
    }

    @Test
    public void query() {
        // 1 条件查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("artitle")
                .setQuery(QueryBuilders.queryStringQuery("全文")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());//打印出每条结果
        }

        // 3 关闭连接
        client.close();
    }

    @Test
    public void wildcardQuery() {

        // 1 通配符查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("artitle")
                .setQuery(QueryBuilders.wildcardQuery("content", "*全*")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());//打印出每条结果
        }

        // 3 关闭连接
        client.close();
    }

    @Test
    public void termQuery() {

        // 1 第一field查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("artitle")
                .setQuery(QueryBuilders.termQuery("content", "全文")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());//打印出每条结果
        }

        // 3 关闭连接
        client.close();
    }

    @Test
    public void fuzzy() {

        // 1 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("artitle")
                .setQuery(QueryBuilders.fuzzyQuery("title", "lucene")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }

        // 3 关闭连接
        client.close();
    }

    @Test
    public void createMapping() throws Exception {

        // 1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id1")
                .field("type", "text")
                .field("store", "yes")
                .endObject()
                .startObject("title2")
                .field("type", "text")
                .field("store", "no")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", "yes")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog4").type("article").source(builder);

        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();
    }

}
