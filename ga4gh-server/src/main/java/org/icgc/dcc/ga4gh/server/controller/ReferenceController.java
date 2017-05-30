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
package org.icgc.dcc.ga4gh.server.controller;

import org.icgc.dcc.ga4gh.server.reference.ReferenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ga4gh.ReferenceServiceOuterClass.GetReferenceRequest;
import ga4gh.ReferenceServiceOuterClass.GetReferenceSetRequest;
import ga4gh.ReferenceServiceOuterClass.ListReferenceBasesRequest;
import ga4gh.ReferenceServiceOuterClass.ListReferenceBasesResponse;
import ga4gh.ReferenceServiceOuterClass.SearchReferenceSetsRequest;
import ga4gh.ReferenceServiceOuterClass.SearchReferenceSetsResponse;
import ga4gh.ReferenceServiceOuterClass.SearchReferencesRequest;
import ga4gh.ReferenceServiceOuterClass.SearchReferencesResponse;
import ga4gh.References.Reference;
import ga4gh.References.ReferenceSet;
import lombok.val;

@RestController
public class ReferenceController {

  @Autowired
  private ReferenceService referenceService;

  @PostMapping("/referencesets/search")
  public SearchReferenceSetsResponse searchReferenceSets(@RequestBody SearchReferenceSetsRequest request) {
    return SearchReferenceSetsResponse.newBuilder().build();
  }

  @GetMapping("/referencesets/{referenceSetId:(?!search).+}")
  public ReferenceSet getReferenceSet(@PathVariable("referenceSetId") String referenceSetId) {
    val request = GetReferenceSetRequest.newBuilder().setReferenceSetId(referenceSetId).build();
    return referenceService.getReferenceSet(request);
  }

  @PostMapping("/references/search")
  public SearchReferencesResponse searchReferences(@RequestBody SearchReferencesRequest request) {
    return SearchReferencesResponse.newBuilder().build();
  }

  @GetMapping("/references/{referenceId:(?!search).+}")
  public Reference getReference(@PathVariable("referenceId") String referenceId) {
    val request = GetReferenceRequest.newBuilder().setReferenceId(referenceId).build();
    return referenceService.getReference(request);
  }

  @GetMapping("/references/{referenceId:(?!search).+}/bases")
  public ListReferenceBasesResponse listReferenceBases(
      @PathVariable("referenceId") String referenceId,
      @RequestParam(name = "start", required = false, defaultValue = "0") Long start,
      @RequestParam(name = "end", required = false) Long end,
      @RequestParam(name = "page_token", required = false, defaultValue = "") String pageToken) {

    Long adjustedEnd = end;
    if (adjustedEnd == null) {
      adjustedEnd = getReference(referenceId).getLength();
    }

    val request = ListReferenceBasesRequest.newBuilder()
        .setReferenceId(referenceId)
        .setStart(start)
        .setEnd(adjustedEnd)
        .setPageToken(pageToken).build();

    return referenceService.listReferenceBases(request);
  }

}
