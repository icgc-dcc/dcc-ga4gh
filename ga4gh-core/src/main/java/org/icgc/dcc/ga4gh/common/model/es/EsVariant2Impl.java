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
package org.icgc.dcc.ga4gh.common.model.es;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.ga4gh.common.AsciiConverters;
import org.icgc.dcc.ga4gh.common.model.es.EsCall.EsCallSerializer;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.ga4gh.common.AsciiConverters.checkPureAscii;
import static org.icgc.dcc.ga4gh.common.AsciiConverters.convertToBytePrimitiveArray;

/**
 * Byte packed implementation of EsVariant. Assumes that referenceBases and alternative bases
 * are ASCII only. Instead of storing 2 Bytes per character for bases, store one.
*/
@ToString
@EqualsAndHashCode(exclude = "calls")
@NoArgsConstructor
public final class EsVariant2Impl implements Serializable, EsVariant2 {

  private static final long serialVersionUID = 1485228376L;
  public static final String TYPE_NAME = "variant";

  public static EsVariant2Impl createEsVariant2(){
    return new EsVariant2Impl();
  }

  private int start;
  private int end;
  private String referenceName;
  private byte[] referenceBases;
  private byte[][] alternativeBases;
  private List<EsCall> calls = newArrayList();

  public EsVariant2Impl setCalls(List<EsCall> calls){
    this.calls = calls;
    return this;
  }

  public EsVariant2Impl addCall(EsCall call){
    calls.add(call);
    return this;
  }

  @Override public List<EsCall> getCalls(){
    return this.calls;
  }

  private byte[] getReferenceBases() {
    return referenceBases;
  }

  private byte[][] getAlternativeBases() {
    return alternativeBases;
  }

  @Override public int getStart() {
    return this.start;
  }

  @Override public int getEnd() {
    return this.end;
  }

  @Override public String getReferenceName() {
    return this.referenceName;
  }

  @Override public String getReferenceBasesAsString() {
    return AsciiConverters.convertToString(referenceBases);
  }

  @Override public ImmutableList<String> getAlternativeBasesAsStrings() {
    return Streams.stream(alternativeBases)
        .map(AsciiConverters::convertToString)
        .collect(toImmutableList());
  }

  @Override public EsVariant2Impl setStart(final int start){
    this.start = start;
    return this;
  }

  @Override public EsVariant2Impl setEnd(final int end){
    this.end = end;
    return this;
  }

  @Override public int numCalls(){
    return getCalls().size();
  }

  @Override public EsVariant2Impl setReferenceName(String referenceName){
    this.referenceName = referenceName;
    return this;
  }

  @Override public EsVariant2Impl setReferenceBases(final String referenceBases) {
    checkPureAscii(referenceBases);
    return setReferenceBases(convertToBytePrimitiveArray(referenceBases));
  }

  @Override public EsVariant2Impl setReferenceBases(final byte[] referenceBases) {
    this.referenceBases = referenceBases;
    return this;
  }


  @Override public EsVariant2Impl setAlternativeBases(final Iterable<? extends String> inputAlternativeBases) {
    val size= Iterables.size(inputAlternativeBases);
    this.alternativeBases = new byte[size][];
    int i = 0;
    for (val base : inputAlternativeBases) {
      checkPureAscii(base);
      this.alternativeBases[i++]= convertToBytePrimitiveArray(base);
    }
    return this;
  }

  @Override public EsVariant2Impl setAlternativeBases(final byte[][] inputAlternativeBases) {
    this.alternativeBases = inputAlternativeBases;
    return this;
  }
  /*
   * Serializer needed for MapDB. Note: if EsVariant member variables are added, removed or modified, this needs to be
   * updated
   */
  public static class EsVariantSerializer implements Serializer<EsVariant2Impl>, Serializable {
    private static final EsCallSerializer ES_CALL_SERIALIZER = new EsCallSerializer();

    @Override
    public void serialize(DataOutput2 out, EsVariant2Impl value) throws IOException {
      out.packInt(value.getStart());
      out.packInt(value.getEnd());
      out.writeUTF(value.getReferenceName());

      BYTE_ARRAY.serialize(out, value.getReferenceBases());

      val doubleArray = value.getAlternativeBases();
      val numAltBases = doubleArray.length;
      out.packInt(numAltBases);
      for (int i = 0; i < numAltBases; i++) {
        byte[] b = doubleArray[i];
        BYTE_ARRAY.serialize(out, b);
      }

      // Serialize calls
      val numCalls = value.numCalls();
      if (numCalls > 0){
        out.packInt(numCalls); // Length
        for (val call : value.getCalls()){
          ES_CALL_SERIALIZER.serialize(out, call);
        }
      } else {
        out.packInt(0); // Length
      }
    }

    @Override
    @SneakyThrows
    public EsVariant2Impl deserialize(DataInput2 input, int available) throws IOException {
      val start = input.unpackInt();
      val end = input.unpackInt();
      val referenceName = input.readUTF();

      // Deserialize ReferenceBases
      val refBasesArray = BYTE_ARRAY.deserialize(input, available);

      // Deserialize AlternateBases
      val altBasesListLength = input.unpackInt();
      byte[][] doubleArray = new byte[altBasesListLength][];
      for (int i = 0; i < altBasesListLength; i++) {
        doubleArray[i] = BYTE_ARRAY.deserialize(input,available);
      }

      val esVariant2 = EsVariant2Impl.createEsVariant2()
          .setStart(start)
          .setEnd(end)
          .setReferenceName(referenceName)
          .setReferenceBases(refBasesArray)
          .setAlternativeBases(doubleArray);

      //Deserialize Calls
      val numCalls = input.unpackInt();
      if (numCalls > 0){
        for (int i =0; i<numCalls; i++){
          val call = ES_CALL_SERIALIZER.deserialize(input, available);
          esVariant2.addCall(call);
        }
      }

      return esVariant2;

    }

  }

}
