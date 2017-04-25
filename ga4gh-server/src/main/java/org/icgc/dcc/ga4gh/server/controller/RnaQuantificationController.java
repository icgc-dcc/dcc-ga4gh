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
package org.icgc.dcc.ga4gh.server.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ga4gh.RnaQuantificationOuterClass.ExpressionLevel;
import ga4gh.RnaQuantificationOuterClass.RnaQuantification;
import ga4gh.RnaQuantificationServiceOuterClass.GetExpressionLevelRequest;
import ga4gh.RnaQuantificationServiceOuterClass.GetRnaQuantificationRequest;
import ga4gh.RnaQuantificationServiceOuterClass.SearchExpressionLevelsRequest;
import ga4gh.RnaQuantificationServiceOuterClass.SearchExpressionLevelsResponse;
import ga4gh.RnaQuantificationServiceOuterClass.SearchRnaQuantificationSetsRequest;
import ga4gh.RnaQuantificationServiceOuterClass.SearchRnaQuantificationSetsResponse;
import lombok.val;

@RestController
public class RnaQuantificationController {

  @PostMapping("/rnaquantificationsets/search")
  public SearchRnaQuantificationSetsResponse searchRnaQuantificationSets(
      @RequestBody SearchRnaQuantificationSetsRequest request) {
    return SearchRnaQuantificationSetsResponse.newBuilder().build();
  }

  @GetMapping("/rnaquantificationsets/{rnaQuantificationId:(?!search).+}")
  public RnaQuantification getRnaQuantification(
      @PathVariable("rnaQuantificationId") GetRnaQuantificationRequest request) {
    val id = request.getRnaQuantificationId();
    return RnaQuantification.newBuilder().setId(id).build();
  }

  @PostMapping("/expressionlevels/search")
  public SearchExpressionLevelsResponse searchExpressionLevels(@RequestBody SearchExpressionLevelsRequest request) {
    return SearchExpressionLevelsResponse.newBuilder().build();
  }

  @GetMapping("/expressionlevels/{expressionLevelId:(?!search).+}")
  public ExpressionLevel getExpressionLevel(@PathVariable("expressionLevelId") GetExpressionLevelRequest request) {
    val id = request.getExpressionLevelId();
    return ExpressionLevel.newBuilder().setId(id).build();
  }

}
