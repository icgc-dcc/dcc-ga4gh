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

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ga4gh.BioMetadata.BioSample;
import ga4gh.BioMetadata.Individual;
import ga4gh.BioMetadataServiceOuterClass.GetBioSampleRequest;
import ga4gh.BioMetadataServiceOuterClass.GetIndividualRequest;
import ga4gh.BioMetadataServiceOuterClass.SearchBioSamplesRequest;
import ga4gh.BioMetadataServiceOuterClass.SearchBioSamplesResponse;
import ga4gh.BioMetadataServiceOuterClass.SearchIndividualsRequest;
import ga4gh.BioMetadataServiceOuterClass.SearchIndividualsResponse;
import lombok.val;

@RestController
public class BioMetadataController {

  @PostMapping("/individuals/search")
  public SearchIndividualsResponse searchIndividuals(@RequestBody SearchIndividualsRequest request) {
    return SearchIndividualsResponse.newBuilder().build();
  }

  @GetMapping("/individuals/{individualId:(?!search).+}")
  public Individual getIndividual(@PathVariable("individualId") GetIndividualRequest request) {
    val id = request.getIndividualId();
    return Individual.newBuilder().setId(id).build();
  }

  @PostMapping("/biosamples/search")
  public SearchBioSamplesResponse searchBioSamples(@RequestBody SearchBioSamplesRequest request) {
    return SearchBioSamplesResponse.newBuilder().build();
  }

  @GetMapping("/biosamples/{bioSampleId:(?!search).+}")
  public BioSample getBioSample(@PathVariable("bioSampleId") GetBioSampleRequest request) {
    val id = request.getBioSampleId();
    return BioSample.newBuilder().setId(id).build();
  }

}
