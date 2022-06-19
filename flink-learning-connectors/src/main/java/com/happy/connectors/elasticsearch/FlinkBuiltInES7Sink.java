package com.happy.connectors.elasticsearch;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * @author happy
 * @since 2022/6/19
 */
public class FlinkBuiltInES7Sink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String ip = "localhost";
        if (args.length == 1) {
            ip = args[0];
        }
        DataStreamSource<String> source = env.socketTextStream(ip, 9999);
        source.sinkTo(new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(1)
                .setBulkFlushInterval(30000L)
                .setBulkFlushMaxSizeMb(5)
                .setConnectionPassword("pwd")
                .setConnectionUsername("username")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter((element, context, indexer) ->
                        indexer.add(createIndexRequest(element)))
                .build()
        );

        env.execute();
    }

    private static IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .id(element)
                .source(json);
    }
}
