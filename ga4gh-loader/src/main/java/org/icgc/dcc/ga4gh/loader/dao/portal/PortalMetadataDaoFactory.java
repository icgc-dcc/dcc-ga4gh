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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;
import org.icgc.dcc.ga4gh.loader.persistance.FileObjectRestorerFactory;
import org.icgc.dcc.ga4gh.loader.portal.Portal;
import org.icgc.dcc.ga4gh.loader.portal.PortalFiles;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.ga4gh.loader.dao.portal.PortalMetadataDao.newPortalMetadataDao;

@RequiredArgsConstructor
public class PortalMetadataDaoFactory {

  @NonNull private final String persistanceName;
  @NonNull private final Portal portal;
  @NonNull private final FileObjectRestorerFactory fileObjectRestorerFactory;

  @SneakyThrows
  public PortalMetadataDao getPortalMetadataDao(){
    val list = fileObjectRestorerFactory.persistObject(persistanceName, this::createObject);
    return newPortalMetadataDao(list);
  }

  private ArrayList<PortalMetadata> createObject() {
    return newArrayList(portal.getFileMetas()
        .stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toList()));
  }

  public static PortalMetadataDaoFactory createPortalMetadataDaoFactory(String persistanceName, Portal portal,
      FileObjectRestorerFactory fileObjectRestorerFactory) {
    return new PortalMetadataDaoFactory(persistanceName, portal, fileObjectRestorerFactory);
  }

}
