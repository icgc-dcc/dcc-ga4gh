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

import org.icgc.dcc.ga4gh.server.variant.VariantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsResponse;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsResponse;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsResponse;
import ga4gh.Variants.CallSet;
import ga4gh.Variants.Variant;
import ga4gh.Variants.VariantSet;

@RestController
public class VariantController {

  @Autowired
  private VariantService variantService;

  @PostMapping("/variants/search")
  public SearchVariantsResponse searchVariants(@RequestBody SearchVariantsRequest request) {
    return variantService.searchVariants(request);
  }

  @GetMapping("/variants/{variantId:(?!search).+}")
  public Variant getVariant(@PathVariable("variantId") GetVariantRequest request) {
    return variantService.getVariant(request);
  }

  @PostMapping("/variantsets/search")
  public SearchVariantSetsResponse searchVariantSets(@RequestBody SearchVariantSetsRequest request) {
    return variantService.searchVariantSets(request);
  }

  @GetMapping("/variantsets/{variantSetId:(?!search).+}")
  public VariantSet getVariantSet(@PathVariable("variantSetId") GetVariantSetRequest request) {
    return variantService.getVariantSet(request);
  }

  @PostMapping("/callsets/search")
  public SearchCallSetsResponse searchCallSets(@RequestBody SearchCallSetsRequest request) {
    return variantService.searchCallSets(request);
  }

  @GetMapping("/callsets/{callSetId:(?!search).+}")
  public CallSet getCallSet(@PathVariable("callSetId") GetCallSetRequest request) {
    return variantService.getCallSet(request);
  }

}
