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

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.ga4gh.common.ObjectNodeConverter;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

@RequiredArgsConstructor(access = PRIVATE)
@Value
@Slf4j
public class PortalAllCollabVcfFileQueryCreator implements ObjectNodeConverter {

  @Override
  public ObjectNode toObjectNode(){
    return object()
        .with("file",
            object()
                .with("repoName", createIs("Collaboratory - Toronto"))
                .with("dataType", createIs("SSM"))
                .with("study", createIs("PCAWG"))
                .with("experimentalStrategy", createIs("WGS"))
                .with("fileFormat", createIs("VCF"))
        )
        .end();
  }

  public static PortalAllCollabVcfFileQueryCreator createPortalAllCollabVcfFileQueryCreator() {
    log.info("Creating PortalAllCollabVcfFileQueryCreator instance");
    return new PortalAllCollabVcfFileQueryCreator();
  }

}
