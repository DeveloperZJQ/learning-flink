package com.happy.connectors.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Properties;

/**
 * @author happy
 * @since 2020-11-12
 */
@Slf4j
public class ESHighRestBulkProcessorWriter extends RichSinkFunction<String> {
    private Properties para;
    private static RestHighLevelClient restHighLevelClient = null;
    private static BulkProcessor bulkProcessor = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        initESClient();
        initBulkProcessor();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String indexName = "test001";
        String type = "banji";
        String id = "123213213";
        bulkProcessor.add(new IndexRequest(indexName,type,id).source(value));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (bulkProcessor!=null){
            bulkProcessor.close();
        }
    }

    void initESClient() {
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("es.cluster.name", "pwd"));

        String[] esHost = "192.168.1.11,192.168.1.12,192.168.1.13".split(",");
        HttpHost[] httpHosts = new HttpHost[esHost.length];

        for (int i = 0; i < esHost.length; i++) {
            httpHosts[i] = new HttpHost(esHost[i], 9200, "http");
        }

        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(httpHosts)
                .setRequestConfigCallback(builder -> {
                    builder.setConnectionRequestTimeout(10000);
                    builder.setConnectTimeout(10000);
                    builder.setSocketTimeout(10000);
                    return builder;
                })
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.disableAuthCaching();
                    httpAsyncClientBuilder.setMaxConnPerRoute(3);
                    httpAsyncClientBuilder.setMaxConnTotal(100);
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
                }).build()
        );
    }


    /**
     * 初始化BulkProcessor
     */
    void initBulkProcessor() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = new ThreadPool(settings);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                log.info("Executing bulk [{}] with {} requests", l, numberOfActions);
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    log.warn("Bulk [{}] executed with failure", l);
                } else {
                    log.info("Bulk [{}] completed in {} milliseconds", l, bulkResponse.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.error("Failed to executed bulk", throwable);
            }
        };
    }
}
