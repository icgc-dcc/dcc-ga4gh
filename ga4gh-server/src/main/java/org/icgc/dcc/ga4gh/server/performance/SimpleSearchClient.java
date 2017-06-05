/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.icgc.dcc.ga4gh.server.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import ga4gh.VariantServiceOuterClass;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.shaded.apache.http.HttpResponse;
import org.elasticsearch.shaded.apache.http.client.HttpClient;
import org.elasticsearch.shaded.apache.http.client.methods.HttpPost;
import org.elasticsearch.shaded.apache.http.entity.StringEntity;
import org.elasticsearch.shaded.apache.http.impl.client.DefaultHttpClient;
import org.icgc.dcc.common.core.json.JsonNodeBuilders;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.elasticsearch.shaded.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;

@Slf4j
public class SimpleSearchClient {

  private static final String VARIANTS = "variants";
  private static final String VARIANT_SEARCH_PATH = VARIANTS+"/search";

  @NonNull private final String host;
  private final int port;

  /**
   * State
   */
  private String variantSearchUrl;
  private HttpPost post;
  private final HttpClient client = new DefaultHttpClient();

  private SimpleSearchClient(String host, int port) {
    this.host = host;
    this.port = port;

    this.variantSearchUrl = getVariantSearchUrl();
    this.post = createJsonPost(variantSearchUrl);
  }

  @SneakyThrows
  public int searchVariants(VariantServiceOuterClass.SearchVariantsRequest request, Stopwatch watch) {
    val jsonRequest = convertToObjectNode(request);
    val entity = new StringEntity(jsonRequest.toString(), APPLICATION_JSON);
    //      val entity = new ByteArrayEntity(jsonRequest.binaryValue(), APPLICATION_JSON);
    post.setEntity(entity);
    HttpResponse response = null;
    try{
      watch.start();
      response = client.execute(post);
    } catch (Throwable t) {
      log.error("Error runnig variantSearch [{}] -- Message: {}\nStackTrace: {}",
          t.getClass().getName(), t.getMessage(), NEWLINE.join(t.getStackTrace()));

    } finally {
      watch.stop();
    }
    BufferedReader rd = new BufferedReader(
        new InputStreamReader(response.getEntity().getContent()));
    val oo = new ObjectMapper();
    val tree = oo.readTree(rd);
    if (tree.has(VARIANTS)){
      val variants = tree.get(VARIANTS);
      return variants.size();
    } else {
      return 0;
    }
  }

  private String getUrl(String path) {
    return String.format("http://%s:%s/%s", host, port, path);
  }

  private String getVariantSearchUrl() {
    return getUrl(VARIANT_SEARCH_PATH);
  }

  public static SimpleSearchClient createSimpleSearchClient(String host, int port) {
    return new SimpleSearchClient(host, port);
  }

  private static HttpPost createJsonPost(String url) {
    return createPost(url, "application/json");
  }

  private static HttpPost createProtobufPost(String url) {
    return createPost(url, "application/protobuf");
  }

  private static HttpPost createPost(String url, String contentType) {
    HttpPost post = new HttpPost(url);
    post.setHeader("Accept", contentType);
    post.setHeader("Content-type", contentType);
    return post;
  }

  private static ObjectNode convertToObjectNode(VariantServiceOuterClass.SearchVariantsRequest request) {
    val callsetlist = Lists.<String>newArrayList();
    for (int i = 0; i < request.getCallSetIdsCount(); i++) {
      callsetlist.add(request.getCallSetIds(i));
    }
    val obj = object()
        .with("start", (int) request.getStart())
        .with("end", (int) request.getEnd())
        .with("reference_name", request.getReferenceName())
        .with("page_size", (int) request.getPageSize())
        .with("variant_set_id", request.getVariantSetId());
    if (!callsetlist.isEmpty()) {
      obj.with("call_set_ids", JsonNodeBuilders.array(callsetlist));
    }
    return obj.end();
  }

}

