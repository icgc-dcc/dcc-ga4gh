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
import static org.collaboratory.ga4gh.loader.utils.AsciiConverters.checkPureAscii;
import static org.collaboratory.ga4gh.loader.utils.AsciiConverters.convertToByteObjectArray;
import static org.collaboratory.ga4gh.loader.utils.AsciiConverters.convertToBytePrimitiveArray;
import static org.collaboratory.ga4gh.loader.utils.AsciiConverters.convertToString;
import static org.collaboratory.ga4gh.loader.utils.AsciiConverters.unboxByteArray;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.END;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.START;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.IOException;
import java.io.Serializable;

import org.icgc.dcc.common.core.util.Joiners;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;

// Byte packed implementation of EsVariant. Assumes that referenceBases and alternative bases
// are ASCII only. Instead of storing 2 Bytes per character for bases, store one. 
@ToString
@EqualsAndHashCode
public class EsByteVariant implements Serializable, EsVariant {

  private static final long serialVersionUID = 1485228376L;

  private int start;
  private int end;
  private String referenceName;
  private byte[] referenceBases;
  private ImmutableList<Byte[]> alternativeBases;

  @Override
  public ObjectNode toDocument() {
    return object()
        .with(START, getStart())
        .with(END, getEnd())
        .with(REFERENCE_NAME, getReferenceName())
        .with(REFERENCE_BASES, getReferenceBases())
        .with(ALTERNATIVE_BASES, convertStrings(getAlternativeBases()))
        .end();
  }

  private EsByteVariant(final int start,
      final int end,
      final String referenceName,
      final byte[] referenceBases,
      final ImmutableList<Byte[]> alternativeBases) {
    this.start = start;
    this.end = end;
    this.alternativeBases = alternativeBases;
    this.referenceName = referenceName;
    this.referenceBases = referenceBases;
  }

  private byte[] getReferenceBasesAsByteArray() {
    return referenceBases;
  }

  private ImmutableList<Byte[]> getAlternativeBasesAsByteArrays() {
    return alternativeBases;
  }

  @Override
  public String getName() {
    return Joiners.UNDERSCORE.join(start, end, referenceName, referenceBases, Joiners.COMMA.join(alternativeBases));
  }

  public static EsByteVariantBuilder builder() {
    return new EsByteVariantBuilder();
  }

  @Override
  public int getStart() {
    return this.start;
  }

  @Override
  public int getEnd() {
    return this.end;
  }

  @Override
  public String getReferenceName() {
    return this.referenceName;
  }

  @Override
  public String getReferenceBases() {
    return convertToString(referenceBases);
  }

  @Override
  public ImmutableList<String> getAlternativeBases() {
    return alternativeBases.stream()
        .map(x -> convertToString(x))
        .collect(toImmutableList());
  }

  /*
   * EsVariantBuilder that can process strings into appropriate byte format.
   */
  @ToString
  @EqualsAndHashCode
  public static class EsByteVariantBuilder implements EsVariant.EsVariantBuilder {

    protected int start;
    protected int end;
    protected String referenceName;
    protected byte[] referenceBases;
    protected ImmutableList.Builder<Byte[]> alternativeBases;

    public EsByteVariantBuilder() {
    }

    @Override
    public EsByteVariantBuilder start(final int start) {
      this.start = start;
      return this;
    }

    @Override
    public EsByteVariantBuilder end(final int end) {
      this.end = end;
      return this;
    }

    @Override
    public EsByteVariantBuilder referenceName(final String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    @Override
    public EsByteVariantBuilder referenceBases(final String referenceBases) {
      checkPureAscii(referenceBases);
      this.referenceBases = convertToBytePrimitiveArray(referenceBases);
      return this;
    }

    @Override
    public EsByteVariantBuilder alternativeBase(final String alternativeBase) {
      checkPureAscii(alternativeBase);
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(convertToByteObjectArray(alternativeBase));
      return this;
    }

    @Override
    public EsByteVariantBuilder alternativeBases(final Iterable<? extends String> alternativeBases) {
      for (val base : alternativeBases) {
        this.alternativeBase(base);
      }
      return this;
    }

    @Override
    public EsByteVariantBuilder clearAlternativeBases() {
      this.alternativeBases = null;
      return this;
    }

    @Override
    public EsByteVariant build() {
      ImmutableList<Byte[]> alternativeBases =
          this.alternativeBases == null ? ImmutableList.<Byte[]> of() : this.alternativeBases
              .build();
      return new EsByteVariant(start, end, referenceName, referenceBases, alternativeBases);
    }

    private EsByteVariantBuilder alternativeBaseAsBytes(final Byte[] alternativeBase) {
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(alternativeBase);
      return this;
    }

    private <T> EsByteVariantBuilder allAlternativeBasesAsBytes(final Iterable<Byte[]> alternativeBases) {
      for (val base : alternativeBases) {
        this.alternativeBaseAsBytes(base);
      }
      return this;
    }

    private EsByteVariantBuilder referenceBasesAsBytes(final byte[] referenceBases) {
      this.referenceBases = referenceBases;
      return this;
    }
  }

  /*
   * Serializer needed for MapDB. Note: if EsVariant member variables are added, removed or modified, this needs to be
   * updated
   */
  public static class EsByteVariantSerializer implements Serializer<EsByteVariant>, Serializable {

    @Override
    public void serialize(DataOutput2 out, EsByteVariant value) throws IOException {
      out.writeInt(value.getStart());
      out.writeInt(value.getEnd());
      out.writeUTF(value.getReferenceName());
      out.writeInt(value.getReferenceBasesAsByteArray().length); // Length
      out.write(value.getReferenceBasesAsByteArray());
      out.writeInt(value.getAlternativeBasesAsByteArrays().size());
      for (Byte[] arrayByte : value.getAlternativeBasesAsByteArrays()) {
        val bArray = unboxByteArray(arrayByte);
        out.writeInt(bArray.length);
        out.write(bArray);
      }
    }

    @Override
    @SneakyThrows
    public EsByteVariant deserialize(DataInput2 input, int available) throws IOException {
      val start = input.readInt();
      val end = input.readInt();
      val referenceName = input.readUTF();

      // Deserialize ReferenceBases
      val referenceBasesLength = input.readInt();
      byte[] refBasesArray = new byte[referenceBasesLength];
      for (int i = 0; i < referenceBasesLength; i++) {
        refBasesArray[i] = input.readByte();
      }

      // Deserialize AlternateBases
      val altBasesListLength = input.readInt();
      val altBasesByteListBuilder = ImmutableList.<Byte[]> builder();
      for (int i = 0; i < altBasesListLength; i++) {
        val arrayLength = input.readInt();
        Byte[] array = new Byte[arrayLength];
        for (int j = 0; j < arrayLength; j++) {
          array[j] = input.readByte();
        }
        altBasesByteListBuilder.add(array);
      }
      val altBasesList = altBasesByteListBuilder.build();

      return builder()
          .start(start)
          .end(end)
          .referenceName(referenceName)
          .referenceBasesAsBytes(refBasesArray)
          .allAlternativeBasesAsBytes(altBasesList)
          .build();
    }

  }

}
