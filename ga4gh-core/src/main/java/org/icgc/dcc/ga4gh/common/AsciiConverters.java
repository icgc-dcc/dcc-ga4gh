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
