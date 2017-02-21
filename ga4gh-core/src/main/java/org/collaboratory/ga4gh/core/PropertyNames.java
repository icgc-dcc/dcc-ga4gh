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
package org.collaboratory.ga4gh.core;

import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class PropertyNames {

  public static final String NAME = "name";
  public static final String BIO_SAMPLE_ID = "bio_sample_id";
  public static final String VARIANT_SET_IDS = "variant_set_ids";
  public static final String VARIANT_SET_ID = "variant_set_id";
  public static final String CALL_SET_ID = "call_set_id";
  public static final String CALL_SET_NAME = "call_set_name";
  public static final String DATA_SET_ID = "data_set_id";
  public static final String REFERENCE_SET_ID = "reference_set_id";
  public static final String START = "start";
  public static final String END = "end";
  public static final String REFERENCE_NAME = "reference_name";
  public static final String INFO = "info";
  public static final String REFERENCE_BASES = "reference_bases";
  public static final String ALTERNATIVE_BASES = "alternate_bases";
  public static final String GENOTYPE_LIKELIHOOD = "genotype_likelihood";
  public static final String GENOTYPE_PHASESET = "genotype_phaseset";
  public static final String NON_REFERENCE_ALLELES = "non_reference_alleles";

  public static final String getAggNameForProperty(final String propertyName){
    return "by_"+propertyName;
  }
}

