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

package org.icgc.dcc.ga4gh.loader.portal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.ga4gh.loader.Config.PORTAL_API;

@Slf4j
@Builder
public class Portal {

  private static final String REPOSITORY_FILES_ENDPOINT = "/api/v1/repository/files";
  private static final Joiner AMPERSAND_JOINER = Joiner.on("&");
  private static final int PORTAL_FETCH_SIZE = 100;
  private static final String HITS = "hits";
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  @NonNull
  private final ObjectNodeConverter jsonQueryGenerator;

  @SneakyThrows
  public URL getUrl(int size, int from) {
    val endpoint = PORTAL_API + REPOSITORY_FILES_ENDPOINT;
    val include = "facets";
    val filters = URLEncoder.encode(jsonQueryGenerator.toObjectNode().toString(), UTF_8.name());
    val urlEnding = AMPERSAND_JOINER.join(
        "include="+include,
        "from="+from,
        "size="+size,
        "filters="+filters
    );
    return new URL(endpoint + "?" + urlEnding);
  }

  public List<ObjectNode> getFileMetas() {
    val fileMetas = ImmutableList.<ObjectNode> builder();
    val size = PORTAL_FETCH_SIZE;
    int from = 1;
    int count = 0;

    while (true) {
      val url = getUrl(size, from);
      val result = read(url);
      val hits = getHits(result);

      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        fileMetas.add(fileMeta);
      }
      log.info("Page: {}", ++count);
      if (hits.size() < size) {
        break;
      }

      from += size;
    }
    return fileMetas.build();
  }

  @SneakyThrows
  private static JsonNode read(URL url) {
    return DEFAULT_MAPPER.readTree(url);
  }

  private static JsonNode getHits(JsonNode result) {
    return result.get(HITS);
  }


}
