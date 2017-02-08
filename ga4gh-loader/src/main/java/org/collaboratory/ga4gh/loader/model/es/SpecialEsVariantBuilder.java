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
package org.collaboratory.ga4gh.loader.model.es;

import static org.collaboratory.ga4gh.core.Names.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.core.Names.END;
import static org.collaboratory.ga4gh.core.Names.REFERENCE_BASES;
import static org.collaboratory.ga4gh.core.Names.REFERENCE_NAME;
import static org.collaboratory.ga4gh.core.Names.START;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToString;
import static org.collaboratory.ga4gh.core.SearchHits.convertHitToStringList;

import java.util.List;

import org.collaboratory.ga4gh.loader.model.es.EsVariant.EsVariantBuilder;
import org.elasticsearch.search.SearchHit;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;

public class SpecialEsVariantBuilder extends EsVariantBuilder {

  public SpecialEsVariantBuilder fromSearchHit(final SearchHit hit) {
    val start = convertHitToInteger(hit, START);
    val end = convertHitToInteger(hit, END);
    val referenceName = convertHitToString(hit, REFERENCE_NAME);
    val referenceBases = convertHitToString(hit, REFERENCE_BASES);
    val alternateBases = convertHitToStringList(hit, ALTERNATIVE_BASES);
    return (SpecialEsVariantBuilder) start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBases(referenceBases)
        .alternativeBases(alternateBases);
  }

  public SpecialEsVariantBuilder fromVariantContext(final VariantContext variantContext) {
    return (SpecialEsVariantBuilder) referenceBases(variantContext.getReference())
        .alternativeBases(variantContext.getAlternateAlleles())
        .start(variantContext.getStart())
        .end(variantContext.getEnd())
        .referenceName(variantContext.getContig());
  }

  public SpecialEsVariantBuilder referenceBases(final Allele allele) {
    return (SpecialEsVariantBuilder) referenceBasesAsBytes(allele.getBases());
  }

  public SpecialEsVariantBuilder alternativeBase(final Allele allele) {
    return (SpecialEsVariantBuilder) alternativeBaseAsBytes(allele.getBases());
  }

  public SpecialEsVariantBuilder alternativeBases(final List<Allele> alleles) {
    alleles.stream().forEach(x -> alternativeBase(x));
    return this;
  }
}