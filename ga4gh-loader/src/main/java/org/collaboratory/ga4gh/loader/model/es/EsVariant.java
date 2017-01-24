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

import static com.google.common.base.Preconditions.checkArgument;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.END;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_BASES;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.START;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.CharsetEncoder;

import org.icgc.dcc.common.core.util.Joiners;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;

// ObjectNode is a bit heavy, this is just to minimize memory usage
@ToString
@EqualsAndHashCode
public final class EsVariant implements EsModel, Serializable {

  private static final long serialVersionUID = 1485228376L;

  private static final CharsetEncoder ASCII_ENCODER =
      Charsets.US_ASCII.newEncoder(); // or "ISO-8859-1" for ISO Latin 1

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
        .with(ALTERNATIVE_BASES, JsonNodeConverters.convertStrings(getAlternativeBases()))
        .end();
  }

  private EsVariant(final int start,
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

  public byte[] getReferenceBasesAsByteArray() {
    return referenceBases;
  }

  public ImmutableList<Byte[]> getAlternativeBasesAsByteArrays() {
    return alternativeBases;
  }

  @Override
  public String getName() {
    return Joiners.UNDERSCORE.join(start, end, referenceName, referenceBases, Joiners.COMMA.join(alternativeBases));
  }

  private static String convertBytesToString(byte[] bytes) {
    return new String(bytes, Charsets.US_ASCII);
  }

  private static String convertBytesToString(Byte[] bytes) {
    return new String(convertToPrimative(bytes), Charsets.US_ASCII);
  }

  public static EsVariantBuilder builder() {
    return new EsVariantBuilder();
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
    return convertBytesToString(referenceBases);
  }

  public ImmutableList<String> getAlternativeBases() {
    return alternativeBases.stream()
        .map(x -> convertBytesToString(x))
        .collect(toImmutableList());
  }

  public static boolean isPureAscii(String v) {
    return ASCII_ENCODER.canEncode(v);
  }

  public static void checkPureAscii(String v) {
    checkArgument(isPureAscii(v),
        "The string [%s] is not of character set [US-ASCII], it needs to be converted", v);
  }

  public static byte[] convertToAsciiPrimitive(String v) {
    return v.getBytes(Charsets.US_ASCII);
  }

  public static Byte[] convertToAsciiObject(String v) {
    return convertToObject(convertToAsciiPrimitive(v));
  }

  public static Byte[] convertToObject(byte[] bytes) {
    val out = new Byte[bytes.length];
    int i = 0;
    for (byte b : bytes) {
      out[i++] = b;
    }
    return out;
  }

  public static byte[] convertToPrimative(Byte[] bytes) {
    val out = new byte[bytes.length];
    int i = 0;
    for (byte b : bytes) {
      out[i++] = b;
    }
    return out;
  }

  /*
   * EsVariantBuilder that can process strings into appropriate byte format.
   */
  @ToString
  @EqualsAndHashCode
  public static class EsVariantBuilder {

    private int start;
    private int end;
    private String referenceName;
    private byte[] referenceBases;
    private ImmutableList.Builder<Byte[]> alternativeBases;

    public EsVariantBuilder() {
    }

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
      EsVariant.checkPureAscii(referenceBases);
      this.referenceBases = convertToAsciiPrimitive(referenceBases);
      return this;
    }

    public EsVariantBuilder referenceBasesAsBytes(final byte[] referenceBases) {
      this.referenceBases = referenceBases;
      return this;
    }

    public EsVariantBuilder alternativeBase(final String alternativeBase) {
      EsVariant.checkPureAscii(alternativeBase);
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(convertToAsciiObject(alternativeBase));
      return this;
    }

    public EsVariantBuilder alternativeBaseAsBytes(final Byte[] alternativeBase) {
      if (this.alternativeBases == null) {
        this.alternativeBases = ImmutableList.<Byte[]> builder();
      }
      this.alternativeBases.add(alternativeBase);
      return this;
    }

    public <T> EsVariantBuilder allAlternativeBasesAsBytes(final Iterable<Byte[]> alternativeBases) {
      for (val base : alternativeBases) {
        this.alternativeBaseAsBytes(base);
      }
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
      ImmutableList<Byte[]> alternativeBases =
          this.alternativeBases == null ? ImmutableList.<Byte[]> of() : this.alternativeBases
              .build();
      return new EsVariant(start, end, referenceName, referenceBases, alternativeBases);
    }
  }

  /*
   * Serializer needed for MapDB. Note: if EsVariant member variables are added, removed or modified, this needs to be
   * updated
   */
  public static class EsVariantSerializer implements Serializer<EsVariant>, Serializable {

    @Override
    public void serialize(DataOutput2 out, EsVariant value) throws IOException {
      out.writeInt(value.getStart());
      out.writeInt(value.getEnd());
      out.writeUTF(value.getReferenceName());
      out.writeInt(value.getReferenceBasesAsByteArray().length); // Length
      out.write(value.getReferenceBasesAsByteArray());
      out.writeInt(value.getAlternativeBasesAsByteArrays().size());
      for (Byte[] arrayByte : value.getAlternativeBasesAsByteArrays()) {
        val bArray = convertToPrimative(arrayByte);
        out.writeInt(bArray.length);
        out.write(bArray);
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

      return EsVariant.builder()
          .start(start)
          .end(end)
          .referenceName(referenceName)
          .referenceBasesAsBytes(refBasesArray)
          .allAlternativeBasesAsBytes(altBasesList)
          .build();
    }

  }

}
