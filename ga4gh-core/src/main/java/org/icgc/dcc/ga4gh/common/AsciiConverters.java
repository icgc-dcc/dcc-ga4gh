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

package org.icgc.dcc.ga4gh.common;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import lombok.NoArgsConstructor;
import lombok.val;

import java.nio.charset.CharsetEncoder;

import static com.google.common.base.Preconditions.checkArgument;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class AsciiConverters {

  private static final CharsetEncoder ASCII_ENCODER =
      Charsets.US_ASCII.newEncoder(); // or "ISO-8859-1" for ISO Latin 1

  public static boolean isPureAscii(String v) {
    return ASCII_ENCODER.canEncode(v);
  }

  public static void checkPureAscii(String v) {
    checkArgument(isPureAscii(v),
        "The string [%s] is not of character set [US-ASCII], it needs to be converted", v);
  }

  public static String convertToString(byte[] bytes) {
    return new String(bytes, Charsets.US_ASCII);
  }

  public static String convertToString(Byte[] bytes) {
    return new String(unboxByteArray(bytes), Charsets.US_ASCII);
  }

  public static byte[] convertToBytePrimitiveArray(String v) {
    return v.getBytes(Charsets.US_ASCII);
  }

  public static Byte[] convertToByteObjectArray(String v) {
    return boxByteArray(convertToBytePrimitiveArray(v));
  }

  public static Byte[] boxByteArray(byte[] bytes) {
    val out = new Byte[bytes.length];
    int i = 0;
    for (byte b : bytes) {
      out[i++] = b;
    }
    return out;
  }

  public static byte[] unboxByteArray(Byte[] bytes) {
    val out = new byte[bytes.length];
    int i = 0;
    for (byte b : bytes) {
      out[i++] = b;
    }
    return out;
  }

  public static byte[][] convertByteArrayList(Iterable<Byte[]> bytes) {
    val size = Iterables.size(bytes);
    val byte2d = new byte[size][];
    int count = 0;
    for (val boxedByte : bytes) {
      byte2d[count] = unboxByteArray(boxedByte);
      count++;
    }
    return byte2d;
  }

}
