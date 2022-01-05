package org.example.kerberos;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ElasticsearchClientUtil {
    /**
     * 如下信息从集群管理平台中下载
     */
    private final static String keytab = "hbasetest.keytab";
    private final static String krbConf = "krb5.conf";

    /**
     * principal可以在集群节点中通过klist方式获取
     */
    private final static String principal = "hbasetest@WH5104.COM";

    /**
     * 假定ES集群的有三个节点
     * 每个节点的信息分别是：
     * 10.121.198.116  端口号 9200
     * 10.121.198.118  端口号 9200
     * 10.121.198.113  端口号 9200
     */
    private final static HttpHost[] esHosts = {
            new HttpHost("10.121.198.116", 9200),
            new HttpHost("10.121.198.118", 9200),
            new HttpHost("10.121.198.113", 9200)
    };

    private RestHighLevelClient esClient;

    public ElasticsearchClientUtil(){
        this.esClient = createEsClient();
    }


    /**
     * 对开启Kerberos的ES集群，创建Client
     */
    private RestHighLevelClient createEsClient(){
        String confPath = this.getClass().getClassLoader().getResource("conf").getPath();

        System.setProperty("java.security.krb5.conf", confPath + File.separator + krbConf);

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(esHosts)
                        .setHttpClientConfigCallback(new SpnegoHttpClientConfigCallbackHandler(principal, confPath + File.separator + keytab, false))
        );
        return client;
    }

    /**
     * 创建索引
     * @param indexName 要创建的索引名字
     * @return 创建索引的响应结果
     * @throws Exception
     */
    public CreateIndexResponse createIndex(String indexName) throws Exception{
        CreateIndexRequest createIndexRequest = new CreateIndexRequest()
                .index(indexName)
                .settings(Settings.builder()
                        .put("number_of_shards", 1)
                        .put("number_of_replicas", 1)
                        .build());

        /**
         * 这里构建索引的mappings信息， 指定索引的字段名字为：conetnt， 类型为text
         */
        XContentBuilder builder = null;
        try{
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("content");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }catch (IOException e){
            throw new Exception("构建索引的mappings信息错误，创建索引失败！");
        }
        createIndexRequest.mapping("_doc", builder);

        CreateIndexResponse response = null;
        try{
            response = this.esClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            throw new Exception(e.getMessage());
        }
        
        return response;
    }

    /**
     * 向指定索引中写入数据
     * @param indexName 索引名
     * @param data   要写入的数据
     * @return 写入响应结果
     * @throws Exception
     */
    public IndexResponse writeIndex(String indexName, Map<String,?> data) throws Exception{
        IndexRequest indexRequest = new IndexRequest()
                .create(true)
                .index(indexName)
                .id("1");
        indexRequest.source(data);

        IndexResponse response = null;
        try{
            response = this.esClient.index(indexRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            throw new Exception(e.getMessage());
        }
        return response;
    }

    /**
     * 查询索引中的数据
     * @param indexName
     * @throws Exception
     */
    public void searchIndex(String indexName) throws Exception{
        SearchRequest searchRequest = new SearchRequest()
                .indices(indexName);

        SearchResponse searchResponse = null;
        try{
            searchResponse = this.esClient.search(searchRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            throw new Exception(e.getMessage());
        }

        for (SearchHit hit : searchResponse.getHits()){
            Map<String, Object> data = hit.getSourceAsMap();
            System.out.println("id : " + hit.getId());
            for (String key : data.keySet()){
                System.out.println("key : " + key + " , value ： " + data.get(key).toString());
            }
        }
    }

    /**
     * 删除指定索引中的指定文档
     * @param indexName 索引名
     * @param id 指定文档的id
     * @return
     * @throws Exception
     */
    public DeleteResponse deleteDoc(String indexName, String id) throws Exception{
        DeleteRequest deleteRequest = new DeleteRequest()
                .index(indexName)
                .id(id);
        DeleteResponse deleteResponse = null;
        try{
            deleteResponse = this.esClient.delete(deleteRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            throw new Exception(e.getMessage());
        }
        return deleteResponse;
    }

    /**
     * 删除指定的索引
     * @param indexName 索引名
     * @return
     * @throws Exception
     */
    public AcknowledgedResponse deleteIndex(String indexName) throws Exception{
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest()
                .indices(indexName);

        AcknowledgedResponse response = null;
        try{
            response = this.esClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            throw new Exception(e.getMessage());
        }
        return response;
    }

    public void close() throws IOException {
        if (this.esClient != null){
            this.esClient.close();
        }
    }
}
