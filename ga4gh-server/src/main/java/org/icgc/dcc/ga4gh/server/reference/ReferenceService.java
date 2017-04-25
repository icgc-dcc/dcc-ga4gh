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
package org.icgc.dcc.ga4gh.server.reference;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.primitives.Longs.min;

import java.util.NoSuchElementException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ga4gh.ReferenceServiceOuterClass.GetReferenceRequest;
import ga4gh.ReferenceServiceOuterClass.GetReferenceSetRequest;
import ga4gh.ReferenceServiceOuterClass.ListReferenceBasesRequest;
import ga4gh.ReferenceServiceOuterClass.ListReferenceBasesResponse;
import ga4gh.References.Reference;
import ga4gh.References.ReferenceSet;
import lombok.NonNull;
import lombok.val;

@Service
public class ReferenceService {

  private static final long PAGE_SIZE = 10;

  @Autowired
  private ReferenceGenome genome;

  public Reference getReference(@NonNull GetReferenceRequest request) {
    val referenceId = request.getReferenceId();
    val reference = genome.getReference(referenceId);

    return Reference.newBuilder()
        .setId(referenceId)
        .setLength(reference.getSequenceLength())
        .setMd5Checksum(reference.getMd5())
        .setName(reference.getSequenceName())
        .setSourceUri("")
        .setIsDerived(false)
        .setNcbiTaxonId(0)
        .setSourceDivergence(0.0f)
        .addSourceAccessions(genome.getVersion())
        .build();
  }

  public ReferenceSet getReferenceSet(@NonNull GetReferenceSetRequest request) {
    val referenceSetId = request.getReferenceSetId();
    if (!genome.getVersion().equals(referenceSetId)) {
      // TODO: Custom exception
      throw new NoSuchElementException(referenceSetId);
    }

    return ReferenceSet.newBuilder()
        .setId(referenceSetId)
        .setAssemblyId(genome.getVersion())
        .setName(genome.getVersion())
        .setDescription(genome.getVersion())
        .setMd5Checksum(genome.getDictionary().md5())
        .setIsDerived(false)
        .setNcbiTaxonId(0)
        .addSourceAccessions(genome.getVersion())
        .build();
  }

  public ListReferenceBasesResponse listReferenceBases(@NonNull ListReferenceBasesRequest request) {
    val start = resolveStart(request);
    val end = resolveEnd(request);
    val nextPageToken = resolveNextPageToken(request, end);

    val sequence = genome.getSequence(request.getReferenceId(), start, end);

    return ListReferenceBasesResponse.newBuilder()
        .setSequence(sequence)
        .setOffset(start)
        .setNextPageToken(nextPageToken).build();
  }

  private long resolveStart(ListReferenceBasesRequest request) {
    val pageToken = request.getPageToken();
    return isNullOrEmpty(pageToken) ? request.getStart() : Long.valueOf(pageToken);
  }

  private long resolveEnd(ListReferenceBasesRequest request) {
    return min(request.getEnd() - 1, resolveStart(request) + PAGE_SIZE - 1);
  }

  private String resolveNextPageToken(ListReferenceBasesRequest request, final long end) {
    return end < request.getEnd() ? String.valueOf(end + 1) : "";
  }

}
