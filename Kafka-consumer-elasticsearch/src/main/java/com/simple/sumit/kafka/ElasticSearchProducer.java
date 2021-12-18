package com.simple.sumit.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
@Slf4j
public class ElasticSearchProducer {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client= createClient();
        String json="{\"developer\":\"sumit2\"}";
        IndexRequest indexRequest= new IndexRequest(
                "twitter",
                "tweets"
        ).source(json, XContentType.JSON);
        IndexResponse indexResponse= client.index(indexRequest,RequestOptions.DEFAULT);
        String id=indexResponse.getId();
        log.info("Tweets produced are:\t"+id);
        System.out.println("Tweets produced are:\t"+id);
        client.close();


    }
    public static RestHighLevelClient createClient(){
        //https://w3i4gt895z:q375xgades@kafka-project-4013611778.us-east-1.bonsaisearch.net:443
        String hostname="kafka-project-4013611778.us-east-1.bonsaisearch.net";
        String username="w3i4gt895z";
        String password="q375xgades";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;

    }

}
