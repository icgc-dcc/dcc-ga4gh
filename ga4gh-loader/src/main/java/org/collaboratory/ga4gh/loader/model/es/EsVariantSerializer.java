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

import java.io.IOException;
import java.io.Serializable;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import lombok.SneakyThrows;
import lombok.val;

/*
 * Serializer needed for MapDB. Note: if EsVariant member variables are added, removed or modified, this needs to be
 * updated
 */
public class EsVariantSerializer implements Serializer<EsVariant>, Serializable {

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