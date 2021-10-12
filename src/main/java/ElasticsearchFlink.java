import com.liuxg.User;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchFlink {
    public static void main(String[] args) {
        // Create Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define a source
        try {
            DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

            DataStream<String> filterSource = source.filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    return !s.contains("hello");
                }
            });

            DataStream<User> transSource = filterSource.map(value -> {
                String[] fields = value.split(",");
                return new User(fields[ 0 ], fields[ 1 ]);
            });

            // Use ESBuilder  to construct an output
            List<HttpHost> hosts = new ArrayList<>();
            hosts.add(new HttpHost("localhost", 9200, "http"));
            ElasticsearchSink.Builder<User> builder = new ElasticsearchSink.Builder<User>(hosts,
                    new ElasticsearchSinkFunction<User>() {
                        @Override
                        public void process(User u, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                            Map<String, String> jsonMap = new HashMap<>();
                            jsonMap.put("id", u.id);
                            jsonMap.put("name", u.name);
                            IndexRequest indexRequest = Requests.indexRequest();
                            indexRequest.index("flink-test");
                            // indexRequest.id("1000");
                            indexRequest.source(jsonMap);
                            requestIndexer.add(indexRequest);
                        }
                    });
            
            // Define a sink
            builder.setBulkFlushMaxActions(1);
            transSource.addSink(builder.build());

            // Execute the transform
            env.execute("flink-es");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
