package org.icgc.dcc.ga4gh.loader;

public abstract class GenomicBaseCodec {

  public abstract char   decodeToChar     (byte input,int numBases );
  public abstract byte[] decodeToByteArray(byte input,int numBases );
  public abstract char   decodeToChar     (int  input,int numBases );
  public abstract byte[] decodeToByteArray(int  input,int numBases );
  public abstract byte[] decodeToByteArray(long input,int numBases );


}
