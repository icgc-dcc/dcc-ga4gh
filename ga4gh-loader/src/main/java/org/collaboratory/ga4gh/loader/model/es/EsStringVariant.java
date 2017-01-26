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

import static org.collaboratory.ga4gh.loader.model.es.JsonNodeConverters.convertStrings;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.END;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.START;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.Value;
import lombok.val;

// String implementation of EsVariant. This is the original, more memory heavy impl
@Value
public final class EsStringVariant implements EsVariant {

  private int start;
  private int end;
  private String referenceName;
  private String referenceBases;
  private ImmutableList<String> alternativeBases;

  @Override
  public ObjectNode toDocument() {
    return object()
        .with(START, start)
        .with(END, end)
        .with(REFERENCE_NAME, referenceName)
        .with(REFERENCE_BASES, referenceBases)
        .with(ALTERNATIVE_BASES, convertStrings(getAlternativeBases()))
        .end();
  }

  @Override
  public String getName() {
    return Joiners.UNDERSCORE.join(start, end, referenceName, referenceBases, Joiners.COMMA.join(alternativeBases));
  }

  public static EsStringVariantBuilder builder() {
    return new EsStringVariantBuilder();
  }

  public static class EsStringVariantBuilder implements EsVariant.EsVariantBuilder {

    private int start;
    private int end;
    private String referenceName;
    private String referenceBases;
    private ImmutableList.Builder<String> alternativeBases;

    public EsStringVariantBuilder() {
    }

    @Override
    public EsVariantBuilder start(int start) {
      this.start = start;
      return this;
    }

    @Override
    public EsVariantBuilder end(int end) {
      this.end = end;
      return this;
    }

    @Override
    public EsVariantBuilder referenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    @Override
    public EsVariantBuilder referenceBases(String referenceBases) {
      this.referenceBases = referenceBases;
      return this;
    }

    @Override
    public EsVariantBuilder alternativeBase(String alternativeBase) {
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<String> builder();
      }
      this.alternativeBases.add(alternativeBase);
      return this;
    }

    @Override
    public EsVariantBuilder alternativeBases(Iterable<? extends String> alternativeBases) {
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<String> builder();
      }
      this.alternativeBases.addAll(alternativeBases);
      return this;
    }

    @Override
    public EsVariantBuilder clearAlternativeBases() {
      this.alternativeBases = null;
      return this;
    }

    @Override
    public EsVariant build() {
      val alternativeBases =
          this.alternativeBases == null ? ImmutableList.<String> of() : this.alternativeBases
              .build();
      return new EsStringVariant(start, end, referenceName, referenceBases, alternativeBases);
    }

  }

}
