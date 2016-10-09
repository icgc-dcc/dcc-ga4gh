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

import ga4gh.ReadServiceOuterClass.GetReadGroupSetRequest;
import ga4gh.ReadServiceOuterClass.SearchReadGroupSetsRequest;
import ga4gh.ReadServiceOuterClass.SearchReadGroupSetsResponse;
import ga4gh.ReadServiceOuterClass.SearchReadsRequest;
import ga4gh.ReadServiceOuterClass.SearchReadsResponse;
import ga4gh.Reads.ReadGroupSet;
import lombok.val;

@RestController
public class ReadController {

	@PostMapping("/readgroupsets/search")
	public SearchReadGroupSetsResponse searchReadGroupSets(@RequestBody SearchReadGroupSetsRequest request) {
		return SearchReadGroupSetsResponse.newBuilder().build();
	}

	@GetMapping("/readgroupsets/{id:(?!search).+}")
	public ReadGroupSet getReadGroupSet(@PathVariable("id") GetReadGroupSetRequest request) {
		val id = request.getReadGroupSetId();
		return ReadGroupSet.newBuilder().setId(id).build();
	}

	@PostMapping("/reads/search")
	public SearchReadsResponse searchReads(@RequestBody SearchReadsRequest request) {
		return SearchReadsResponse.newBuilder().build();
	}

}
