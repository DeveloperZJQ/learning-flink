package com.happy.connectors.elasticsearch;

import com.happy.connectors.elasticsearch.bean.CsdnBlog;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.Index;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.PutMapping;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * @author happy
 * @since 2020-11-03 23:53
 */
@Slf4j
public class ESOperator {
    public static void main(String[] args) {
        ESOperator esOperator = new ESOperator();
        esOperator.init();
//        esOperator.health();
//        esOperator.deleteIndex("test_123");
//        esOperator.createIndex("test_123");
//        esOperator.createIndexMapping("test_123","111a");
        for (int i=0;i<10000;i++){
            esOperator.insertMultiple(String.valueOf(i));
        }
    }

    void insertMultiple(String str) {
        CsdnBlog csdnBlog1 = new CsdnBlog();
        csdnBlog1.setAuthor(str+"AAAA");
//
        csdnBlog1.setTitile(str+"中国获租巴土地 为期"+str+"年");
        csdnBlog1.setContent(str+"据了解，瓜另外"+str+"亩的征收");
        csdnBlog1.setDate(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
        csdnBlog1.setView(str+"100");
        csdnBlog1.setTag(str+"JAVA,ANDROID,C++,LINUX");

        Index index1 = new Index.Builder(csdnBlog1).index("test_123").type("111a").build();
        JestResult jestResult1 = null;
        try {
            jestResult1 = jestClient.execute(index1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean succeeded = jestResult1.isSucceeded();
        if (succeeded){
            log.info("insert succeed.");
        }else {
            log.error("insert failed again insert.");
            insertMultiple(str);
        }
    }

    private JestClient jestClient;
    void init() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://106.12.2.73:9200")
                        .multiThreaded(true)
                        .defaultMaxTotalConnectionPerRoute(10)
                        .maxTotalConnection(20)
                        .readTimeout(3000)
                        .connTimeout(1500)
                        .maxConnectionIdleTime(10, TimeUnit.SECONDS)
                        .build());
        jestClient = factory.getObject();
    }

    /**
     * 创建索引
     * @param indexName
     * @throws Exception
     */
    void createIndex(String indexName) {
        JestResult jr = null;
        try {
            jr = jestClient.execute(new CreateIndex.Builder(indexName).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert jr != null;
        boolean succeeded = jr.isSucceeded();
        if (succeeded){
            log.info("create index succeed.");
        }else {
            log.error("execute create index operator failed.");
        }
    }

    void createIndexMapping(String indexName,String typeName) {
        String source = "{\"" + typeName + "\":{\"properties\":{"
                + "\"author\":{\"type\":\"string\",\"index\":\"not_analyzed\"}"
                + ",\"title\":{\"type\":\"string\"}"
                + ",\"content\":{\"type\":\"string\"}"
                + ",\"price\":{\"type\":\"string\"}"
                + ",\"view\":{\"type\":\"string\"}"
                + ",\"tag\":{\"type\":\"string\"}"
                + ",\"date\":{\"type\":\"date\",\"format\":\"yyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\"}"
                + "}}}";
        System.out.println(source);

        PutMapping putMapping = new PutMapping.Builder(indexName, typeName, source).build();
        JestResult jr = null;
        try {
            jr = jestClient.execute(putMapping);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(jr.isSucceeded());
    }

    /**
     * 删除索引
     * @param indexName
     * @throws Exception
     */
    void deleteIndex(String indexName)  {
        JestResult jr = null;
        try {
            jr = jestClient.execute(new DeleteIndex.Builder(indexName).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean result = jr.isSucceeded();
        if (result){
            log.info("delete index succeed.");
        }else {
            log.error("execute delete operator failed.");
        }
    }

    /**
     * 删除所有索引
     * @param indexName
     * @throws Exception
     */
    void deleteIndexAll(String indexName) throws Exception {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();
        JestResult result = jestClient.execute(deleteIndex);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("delete all index succeed.");
        }else {
            log.error("execute all delete operator failed.");
        }
    }

    /**
     * 清除缓存
     * @throws Exception
     */
    void clearCache() throws Exception {
        ClearCache closeIndex = new ClearCache.Builder().build();
        JestResult result = jestClient.execute(closeIndex);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("clear cache.");
        }else {
            log.error("clear cache failed.");
        }
    }


    /**
     * 索引优化器
     * @throws Exception
     */
    void optimize() throws Exception {
        Optimize optimize = new Optimize.Builder().build();
        JestResult result = jestClient.execute(optimize);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("clear cache.");
        }else {
            log.error("clear cache failed.");
        }
    }

    /**
     * 关闭index
     * @param indexName
     * @throws Exception
     */
    void closeIndex(String indexName) throws Exception {
        CloseIndex closeIndex = new CloseIndex.Builder(indexName).build();
        JestResult result = jestClient.execute(closeIndex);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("close index succeed.");
        }else {
            log.error("execute close operator failed.");
        }
    }

    /**
     * 查看index是否存在
     * @param indexName
     * @throws Exception
     */
    void indicesExists(String indexName) throws Exception {
        IndicesExists indicesExists = new IndicesExists.Builder(indexName).build();
        JestResult result = jestClient.execute(indicesExists);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("indices exists succeed.");
        }else {
            log.error("execute indices exists operator failed.");
        }
    }

    /**
     * 刷新index
     * @throws Exception
     */
    void flush() throws Exception {
        Flush flush = new Flush.Builder().build();
        JestResult result = jestClient.execute(flush);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("flush index succeed.");
        }else {
            log.error("execute flush operator failed.");
        }
    }

    /**
     * 查看节点健康状态
     * @throws Exception
     */
    void nodesInfo() throws Exception {
        NodesInfo nodesInfo = new NodesInfo.Builder().build();
        JestResult result = jestClient.execute(nodesInfo);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("nodesInfo succeed.");
        }else {
            log.error("execute nodesInfo operator failed.");
        }
    }

    /**
     * 集群健康状态
     * @throws Exception
     */
    void health(){
        Health health = new Health.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(health);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert result != null;
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("health succeed.");
        }else {
            log.error("execute health operator failed.");
        }
    }

    /**
     * 节点健康状态信息
     * @throws Exception
     */
    void nodesStats() throws Exception {
        NodesStats nodesStats = new NodesStats.Builder().build();
        JestResult result = jestClient.execute(nodesStats);
        boolean succeeded = result.isSucceeded();
        if (succeeded){
            log.info("nodesStats health succeed.");
        }else {
            log.error("execute nodesStats health operator failed.");
        }
    }
}
