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
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.ga4gh.common.AsciiConverters;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.core.util.Joiners.COMMA;
import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.ga4gh.common.MapDBSerialzers.deserializeDoubleByteArray;
import static org.icgc.dcc.ga4gh.common.MapDBSerialzers.serializeArray;

/**
 * Byte packed implementation of EsVariant. Assumes that referenceBases and alternative bases
 * are ASCII only. Instead of storing 2 Bytes per character for bases, store one.
*/
@ToString
@EqualsAndHashCode
public final class EsVariant implements Serializable, EsModel {

  private static final long serialVersionUID = 1485228376L;
  public static final String TYPE_NAME = "variant";

  public static EsVariantBuilder builder() {
    return new EsVariantBuilder();
  }

  private int start;
  private int end;
  private String referenceName;
  private byte[] referenceBases;
  private byte[][] alternativeBases;

  private EsVariant(final int start,
      final int end,
      final String referenceName,
      final byte[] referenceBases,
      final byte[][] alternativeBases) {
    this.start = start;
    this.end = end;
    this.alternativeBases = alternativeBases;
    this.referenceName = referenceName;
    this.referenceBases = referenceBases;
  }

  private byte[] getReferenceBasesAsByteArray() {
    return referenceBases;
  }

  private byte[][] getAlternativeBasesAsByteArrays() {
    return alternativeBases;
  }

  @Override
  public String getName() {
    return UNDERSCORE.join(start, getEnd(), referenceName, referenceBases, COMMA.join(alternativeBases));
  }

  public int getStart() {
    return this.start;
  }

  public int getEnd() {
    return this.end;
  }

  public String getReferenceName() {
    return this.referenceName;
  }

  public String getReferenceBases() {
    return AsciiConverters.convertToString(referenceBases);
  }

  public ImmutableList<String> getAlternativeBases() {
    return Streams.stream(alternativeBases)
        .map(x -> AsciiConverters.convertToString(x))
        .collect(toImmutableList());
  }

  /*
   * EsVariantBuilder that can process strings into appropriate byte format.
   */
  @ToString
  @EqualsAndHashCode
  public static class EsVariantBuilder {

    protected int start;
    protected int end;
    protected String referenceName;
    protected byte[] referenceBases;
    protected ImmutableList.Builder<Byte[]> alternativeBases;

    public EsVariantBuilder start(final int start) {
      this.start = start;
      return this;
    }

    public EsVariantBuilder end(final int end) {
      this.end = end;
      return this;
    }

    public EsVariantBuilder referenceName(final String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public EsVariantBuilder referenceBases(final String referenceBases) {
      AsciiConverters.checkPureAscii(referenceBases);
      this.referenceBases = AsciiConverters.convertToBytePrimitiveArray(referenceBases);
      return this;
    }

    public EsVariantBuilder alternativeBase(final String alternativeBase) {
      AsciiConverters.checkPureAscii(alternativeBase);
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(AsciiConverters.convertToByteObjectArray(alternativeBase));
      return this;
    }

    public EsVariantBuilder alternativeBases(final Iterable<? extends String> alternativeBases) {
      for (val base : alternativeBases) {
        this.alternativeBase(base);
      }
      return this;
    }

    public EsVariantBuilder clearAlternativeBases() {
      this.alternativeBases = null;
      return this;
    }

    public EsVariant build() {
      byte[][] b = null;
      checkNotNull(this.alternativeBases, "AlternativeBases parameter should not be null");
      val altBases = this.alternativeBases.build();
      b = new byte[altBases.size()][];
      for (int i = 0; i < altBases.size(); i++) {
        b[i] = AsciiConverters.unboxByteArray(altBases.get(i));
      }
      return new EsVariant(start, end, referenceName, referenceBases, b);
    }

    public EsVariantBuilder alternativeBaseAsBytes(final byte[] alternativeBase) {
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(AsciiConverters.boxByteArray(alternativeBase));
      return this;
    }

    public EsVariantBuilder allAlternativeBasesAsBytes(final byte[][] alternativeBases) {
      for (val base : alternativeBases) {
        this.alternativeBaseAsBytes(base);
      }
      return this;
    }

    public EsVariantBuilder referenceBasesAsBytes(final byte[] referenceBases) {
      this.referenceBases = referenceBases;
      return this;
    }

  }

  /*
   * Serializer needed for MapDB. Note: if EsVariant member variables are added, removed or modified, this needs to be
   * updated
   */
  public static class EsVariantSerializer implements Serializer<EsVariant>, Serializable {

    @Override
    public void serialize(DataOutput2 out, EsVariant value) throws IOException {
      out.packInt(value.getStart());
      out.packInt(value.getEnd());
      out.writeUTF(value.getReferenceName());
      BYTE_ARRAY.serialize(out, value.getReferenceBasesAsByteArray());
      serializeArray(out, BYTE_ARRAY, value.getAlternativeBasesAsByteArrays());
    }

    @Override
    @SneakyThrows
    public EsVariant deserialize(DataInput2 input, int available) throws IOException {
      val start = input.unpackInt();
      val end = input.unpackInt();
      val referenceName = input.readUTF();
      val refBasesArray = BYTE_ARRAY.deserialize(input, available);
      val alternativeBasesDoubleArray = deserializeDoubleByteArray(input, available, BYTE_ARRAY);
      return EsVariant.builder()
          .start(start)
          .end(end)
          .referenceName(referenceName)
          .referenceBasesAsBytes(refBasesArray)
          .allAlternativeBasesAsBytes(alternativeBasesDoubleArray)
          .build();
    }

  }

  public static class EsVariantOldSerializer implements Serializer<EsVariant>, Serializable {

    @Override
    public void serialize(DataOutput2 out, EsVariant value) throws IOException {
      out.writeInt(value.getStart());
      out.writeInt(value.getEnd());
      out.writeUTF(value.getReferenceName());

      out.writeInt(value.getReferenceBasesAsByteArray().length); // Length
      out.write(value.getReferenceBasesAsByteArray());

      val doubleArray = value.getAlternativeBasesAsByteArrays();
      val numAltBases = doubleArray.length;
      out.writeInt(numAltBases);
      for (int i = 0; i < numAltBases; i++) {
        byte[] b = doubleArray[i];
        out.writeInt(b.length);
        out.write(b);
      }
    }

    @Override
    @SneakyThrows
    public EsVariant deserialize(DataInput2 input, int available) throws IOException {
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
      byte[][] doubleArray = new byte[altBasesListLength][];
      for (int i = 0; i < altBasesListLength; i++) {
        val arrayLength = input.readInt();
        byte[] array = new byte[arrayLength];
        for (int j = 0; j < arrayLength; j++) {
          array[j] = input.readByte();
        }
        doubleArray[i] = array;
      }

      return EsVariant.builder()
          .start(start)
          .end(end)
          .referenceName(referenceName)
          .referenceBasesAsBytes(refBasesArray)
          .allAlternativeBasesAsBytes(doubleArray)
          .build();
    }

  }

}
