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
package org.collaboratory.ga4gh.server.converter;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Set;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;

import com.google.common.collect.ImmutableSet;

import ga4gh.AlleleAnnotationServiceOuterClass.GetVariantAnnotationSetRequest;
import ga4gh.BioMetadataServiceOuterClass.GetBioSampleRequest;
import ga4gh.BioMetadataServiceOuterClass.GetIndividualRequest;
import ga4gh.MetadataServiceOuterClass.GetDatasetRequest;
import ga4gh.ReadServiceOuterClass.GetReadGroupSetRequest;
import ga4gh.ReferenceServiceOuterClass.GetReferenceRequest;
import ga4gh.ReferenceServiceOuterClass.GetReferenceSetRequest;
import ga4gh.ReferenceServiceOuterClass.ListReferenceBasesRequest;
import ga4gh.RnaQuantificationServiceOuterClass.GetExpressionLevelRequest;
import ga4gh.RnaQuantificationServiceOuterClass.GetRnaQuantificationRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.GetFeatureRequest;
import ga4gh.SequenceAnnotationServiceOuterClass.GetFeatureSetRequest;
import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import lombok.SneakyThrows;
import lombok.val;

//@Component
public class StringToRequestConverter implements GenericConverter {

	@Override
	public Set<ConvertiblePair> getConvertibleTypes() {
		val types = ImmutableSet.<Class<?>>of(GetBioSampleRequest.class, GetCallSetRequest.class,
				GetDatasetRequest.class, GetExpressionLevelRequest.class, GetFeatureRequest.class,
				GetFeatureSetRequest.class, GetIndividualRequest.class, GetReadGroupSetRequest.class,
				GetReferenceRequest.class, GetReferenceSetRequest.class, GetRnaQuantificationRequest.class,
				GetVariantAnnotationSetRequest.class, GetVariantRequest.class, GetVariantSetRequest.class,
				ListReferenceBasesRequest.class);

		return types.stream().map(targetType -> new ConvertiblePair(String.class, targetType))
				.collect(toImmutableSet());
	}

	@Override
	@SneakyThrows
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		val newBuilderMethod = targetType.getClass().getMethod("newBuilder");
		val target = newBuilderMethod.invoke(null);
		target.getClass().getFields();

		return source;
	}

}