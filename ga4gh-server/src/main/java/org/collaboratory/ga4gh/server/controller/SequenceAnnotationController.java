/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.collaboratory.ga4gh.server.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ga4gh.SequenceAnnotationServiceOuterClass.GetFeatureRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.GetFeatureSetRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.SearchFeatureSetsRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.SearchFeatureSetsResponse;
import ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.SearchFeaturesResponse;
import ga4gh.SequenceAnnotations.Feature;
import ga4gh.SequenceAnnotations.FeatureSet;
import lombok.val;

@RestController
public class SequenceAnnotationController {

	@PostMapping("/featuresets/search")
	public SearchFeatureSetsResponse searchFeatureSets(@RequestBody SearchFeatureSetsRequest request) {
		return SearchFeatureSetsResponse.newBuilder().build();
	}

	@GetMapping("/featuresets/{featureSetId:(?!search).+}")
	public FeatureSet getFeatureSet(@PathVariable("featureSetId") GetFeatureSetRequest request) {
		val id = request.getFeatureSetId();
		return FeatureSet.newBuilder().setId(id).build();
	}

	@PostMapping("/features/search")
	public SearchFeaturesResponse searchFeatures(@RequestBody SearchFeaturesRequest request) {
		return SearchFeaturesResponse.newBuilder().build();
	}

	@GetMapping("/features/{featureId:(?!search).+}")
	public Feature getFeature(@PathVariable("featureId") GetFeatureRequest request) {
		val id = request.getFeatureId();
		return Feature.newBuilder().setId(id).build();
	}

}
