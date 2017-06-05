package org.icgc.dcc.ga4gh.loader;

import com.google.common.base.Stopwatch;
import ga4gh.MetadataServiceOuterClass;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.shaded.apache.http.HttpResponse;
import org.elasticsearch.shaded.apache.http.client.HttpClient;
import org.elasticsearch.shaded.apache.http.client.methods.HttpPost;
import org.elasticsearch.shaded.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.shaded.apache.http.entity.ContentType;
import org.elasticsearch.shaded.apache.http.impl.client.DefaultHttpClient;
import org.icgc.dcc.ga4gh.server.performance.SimpleSearchClient;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

@Slf4j
@Ignore
public class ReqTest {

  @Test
  @SneakyThrows
  public void testReq3() {
    val host = "localhost";
    val port = 8080;

   val client = SimpleSearchClient.createSimpleSearchClient(host,port);
    val req = SearchVariantsRequest.newBuilder()
        .setStart(3451069)
        .setEnd(93451070)
        .setReferenceName("11")
        .setPageSize(10)
        .setVariantSetId("1")
        .addCallSetIds("551")
        .build();
    val watch = Stopwatch.createUnstarted();
   val numResults = client.searchVariants(req, watch);
   log.info("NumResults: {}", numResults);
  }

  @Test
  @SneakyThrows
  public void testReq1(){
    val host = "localhost";
    val port = "9300";
    val index = "dcc-variants-20170530_095520";
    val type = "variant";
    val urlString = String.format("http://%s:%s/%s/%s/_search", host, port, index, type);
    URL url = new URL(urlString);
    URLConnection con = url.openConnection();
    HttpURLConnection http = (HttpURLConnection) con;
    http.setRequestMethod("POST"); // PUT is another valid option
    http.setDoOutput(true);

//    val proto = ContentType.APPLICATION_OCTET_STREAM.toString();
        val proto = "application/x-protobuf";
    val req = SearchVariantsRequest.newBuilder()
        .setStart(1)
        .setEnd(10000000)
        .setReferenceName("1")
        .setPageSize(10)
        .setVariantSetId("0")
        .build();
    val length = req.toByteArray().length;


    http.setFixedLengthStreamingMode(length);
    http.setRequestProperty("Content-Type", proto+"; charset=UTF-8");
    http.connect();
    http.getOutputStream().write(req.toByteArray());
    val in = http.getInputStream();
    val resp = SearchVariantsResponse.parseFrom(in);
    log.info("resp: {}", resp.toString());

  }

  @Test
  @SneakyThrows
  public void testReq(){
    HttpClient client = new DefaultHttpClient();
    val host = "localhost";
    val port = "9200";
    val index = "dcc-variants-20170523_164610";
    val type = "dataset";
    val url = String.format("http://%s:%s/%s/%s/_search", host, port, index, type);
    HttpPost post = new HttpPost(url);
    // add header
//    post.setHeader("Accept", "application/json");
//    post.setHeader("Content-type", "application/json");
    val proto = "application/x-protobuf";
//    val proto = ContentType.APPLICATION_OCTET_STREAM.toString();
    post.setHeader("Accept", proto);
    post.setHeader("Content-type", proto);

    val data = MetadataServiceOuterClass.SearchDatasetsRequest.newBuilder().setPageSize(10).build();

    val out = new ByteArrayOutputStream();
    data.writeTo(out);
    val entity = new ByteArrayEntity(out.toByteArray(), ContentType.create(proto));


    post.setEntity(entity);

    HttpResponse response = client.execute(post);
    System.out.println("\nSending 'POST' request to URL : " + url);
    System.out.println("Post parameters : " + post.getEntity());
    System.out.println("Response Code : " +
        response.getStatusLine().getStatusCode());

    BufferedReader rd = new BufferedReader(
        new InputStreamReader(response.getEntity().getContent()));

    StringBuffer result = new StringBuffer();
    String line = "";
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }

    System.out.println(result.toString());


  }

}
