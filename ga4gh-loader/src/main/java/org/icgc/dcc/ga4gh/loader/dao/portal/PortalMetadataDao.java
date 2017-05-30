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

package org.icgc.dcc.ga4gh.loader.dao.portal;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.dao.BasicDao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@RequiredArgsConstructor
public class PortalMetadataDao implements BasicDao<PortalMetadata, PortalMetadataRequest>, Serializable {

  public static final long serialVersionUID = 1492088726L;

  @NonNull private final List<PortalMetadata> data;

  private Stream<PortalMetadata> getStream(PortalMetadataRequest request){
    return data.stream()
        .filter(x -> x.getPortalFilename().equals(request.getPortalFilename()));
  }

  @Override public List<PortalMetadata> find(PortalMetadataRequest request) {
    return getStream(request).collect(toImmutableList());
  }

  @Override public List<PortalMetadata> findAll() {
    return ImmutableList.copyOf(data);
  }

  public Optional<PortalMetadata> findFirst(PortalMetadataRequest request){
    return getStream(request).findFirst();
  }

  public Map<String, List<PortalMetadata>> groupBySampleId(){
    return groupBy(PortalMetadata::getSampleId);
  }

  public <T> Map<T, List<PortalMetadata>> groupBy(Function<PortalMetadata, T> functor){
    return findAll().stream()
        .collect(groupingBy(functor));
  }

  public static final PortalMetadataDao newPortalMetadataDao(List<PortalMetadata> data){
    return new PortalMetadataDao(data);
  }

}
