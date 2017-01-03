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
package org.collaboratory.ga4gh.server.util;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

import java.util.Set;

import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;

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
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

@Component
public class StringToGetRequestConverter implements GenericConverter {

  @Getter
  private final Set<ConvertiblePair> convertibleTypes = resolveConvertableTypes();

  @Override
  public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
    val property = resolveProperty(targetType);
    val target = invokeNewBuilder(targetType);
    invokeSetter(target, property, source);
    return invokeBuild(target);
  }

  private String resolveProperty(TypeDescriptor targetType) {
    val pathVariable = targetType.getAnnotation(PathVariable.class);
    return pathVariable.name();
  }

  @SneakyThrows
  private Object invokeNewBuilder(TypeDescriptor targetType) {
    val newBuilderMethod = targetType.getObjectType().getMethod("newBuilder");
    return newBuilderMethod.invoke(null);
  }

  private void invokeSetter(Object target, String property, Object value) {
    val accessor = PropertyAccessorFactory.forBeanPropertyAccess(target);
    accessor.setPropertyValue(property, value);
  }

  @SneakyThrows
  private Object invokeBuild(Object target) {
    return target.getClass().getMethod("build").invoke(target);
  }

  private static Set<ConvertiblePair> resolveConvertableTypes() {
    return ImmutableSet.<Class<?>> of(
        GetBioSampleRequest.class,
        GetCallSetRequest.class,
        GetDatasetRequest.class,
        GetExpressionLevelRequest.class,
        GetFeatureRequest.class,
        GetFeatureSetRequest.class,
        GetIndividualRequest.class,
        GetReadGroupSetRequest.class,
        GetReferenceRequest.class,
        GetReferenceSetRequest.class,
        GetRnaQuantificationRequest.class,
        GetVariantAnnotationSetRequest.class,
        GetVariantRequest.class,
        GetVariantSetRequest.class,
        ListReferenceBasesRequest.class)
        .stream()
        .map(targetType -> new ConvertiblePair(String.class, targetType))
        .collect(toImmutableSet());
  }

}
