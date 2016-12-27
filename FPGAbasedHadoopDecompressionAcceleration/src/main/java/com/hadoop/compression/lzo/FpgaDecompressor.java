/**
 * Created by Chen Qian on 1/20/2016.
 * intel Corp.
 */

package com.hadoop.compression.lzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.zip.Checksum;

import org.apache.hadoop.io.compress.Decompressor;

//public class FpgaDecompressor extends DecompressorAbstract{
public class FpgaDecompressor implements Decompressor {
    private final EnumMap<DChecksum,Checksum> chkDMap = new EnumMap<DChecksum,Checksum>(DChecksum.class);
    private final EnumMap<CChecksum,Checksum> chkCMap = new EnumMap<CChecksum,Checksum>(CChecksum.class);
    private static final Log LOG = LogFactory.getLog(LzoDecompressor.class.getName());
    private static Class clazz = FpgaDecompressor.class;
    private static boolean FpgaLzoLoaded;
    private int directBufferSize;
    private Buffer compressedDirectBuf = null;
    private int compressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finished;
    
    private long fpgaDecompressor = 0;
    private static Class FpgaEnc = null;

    // Whether or not the current block being is actually stored uncompressed.
    // This happens when compressing a block would increase it size.
    private boolean isCurrentBlockUncompressed;
    public int uncompressedDirectBufLen = 0;
    static {
	    //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: try fpga lib loading");
            int itg=9;
            FpgaLibraryWrapper flw = new FpgaLibraryWrapper();
            int iii = flw.intMethod(itg);
            //System.out.println("this.intMethod("+itg+") is "+iii);
            //System.out.println("native wrapper validated now load fpga library");
            if(FpgaLibraryLoader.isFpgaLibraryLoaded()){
              //Initiate the FPGA library
              try{
                  initIDs();
                  FpgaLzoLoaded = true;
                  //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor:  dec java fpga lzo library loaded!");
              } catch (Throwable t){
                  LOG.warn(t.toString());
                  FpgaLzoLoaded = false;
		  //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: dec java fail to load fpga lzo library!");
              }
            } else {
              //System.out.println("dec java fpga lzo library load failed!");
              LOG.error("Can't load FPGA LZO library!");
              FpgaLzoLoaded = false;
        }
    }
    //@Override
    public  FpgaDecompressor(int directBufferSize){
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: construct FpgaDecompressor object! with directBufferSize "+directBufferSize);
        this.directBufferSize = directBufferSize;
        //this.strategy = strategy;

        compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: uncompressedDirectBuf.remaining() is "+uncompressedDirectBuf.remaining());
        uncompressedDirectBuf.position(directBufferSize);
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: uncompressedDirectBuf.remaining() after position is "+uncompressedDirectBuf.remaining());
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: before init() fpgaDecompressor is "+fpgaDecompressor);
        init();//native hook~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: after init() fpgaDecompressor is "+fpgaDecompressor);
    }

    public  static boolean isFpgaLzoLoaded(){
        return FpgaLzoLoaded;
    }

    public  void initHeaderFlags(EnumSet<DChecksum> dflags,
                                EnumSet<CChecksum> cflags) {
        try {
            for (DChecksum flag : dflags) {
                chkDMap.put(flag, flag.getChecksumClass().newInstance());
            }
            for (CChecksum flag : cflags) {
                chkCMap.put(flag, flag.getChecksumClass().newInstance());
            }
        } catch (InstantiationException e) {
            throw new RuntimeException("Internal error", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Internal error", e);
        }
    }

  public /*synchronized*/ void setInput(byte[] b, int off, int len) {
    //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: >>>>>>>>>>setInput(b,off="+off+",len="+len+")");
    if (!isCurrentBlockUncompressed()) {
      if (len > directBufferSize) {
        LOG.warn("Decompression will fail because compressed buffer size :" +
          len + " is greater than this decompressor's directBufferSize: " + 
          directBufferSize + ". To fix this, increase the value of your " + 
          "configuration's io.compression.codec.lzo.buffersize to be larger " +
          "than: " + len + ".");
      }
    }

    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;

    setInputFromSavedData();

    // Reinitialize lzo's output direct-buffer 
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }


    synchronized void setInputFromSavedData() {
      // If the current block is stored uncompressed, no need
      // to ready all the lzo machinery, because it will be bypassed.
      //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: setInputFromSavedData()");
      if (!isCurrentBlockUncompressed()) {
        compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

        // Reinitialize lzo's input direct-buffer
        compressedDirectBuf.rewind();
        ((ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff,
            compressedDirectBufLen);

        // Note how much data is being fed to lzo
        userBufOff += compressedDirectBufLen;
        userBufLen -= compressedDirectBufLen;
      }
    }

    public synchronized boolean needsInput() {
      //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: needsInput() triggred");
      // Consume remaining compressed data?
      if (uncompressedDirectBuf.remaining() > 0) {
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: uncompressedDirectBuf.remaining() > 0)");
        return false;
      }

      // Check if lzo has consumed all input
      if (compressedDirectBufLen <= 0) {
        // Check if we have consumed all user-input
        if (userBufLen <= 0) {
          //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: userBufLen <= 0");
          return true;
        } else {
          //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: to enter setInputFromSavedData()");
          setInputFromSavedData();
        }
      }

      return false;
    }

    public synchronized boolean finished() {
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: finished() triggered");
        // Check if 'lzo' says its 'finished' and
        // all uncompressed data has been consumed
        return (finished && uncompressedDirectBuf.remaining() == 0);
    }
    public synchronized int decompress(byte[] b, int off, int len)
            throws IOException {
	//System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: enter decompress function");
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: b.length is "+b.length+" off is "+off+" len is "+len);
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: trace point1");
        int numBytes = 0;
        if (isCurrentBlockUncompressed()) {
            // The current block has been stored uncompressed, so just
            // copy directly from the input buffer.
            //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: ??????????????current block has been stored uncompressed");
            numBytes = Math.min(userBufLen, len);
            System.arraycopy(userBuf, userBufOff, b, off, numBytes);
            userBufOff += numBytes;
            userBufLen -= numBytes;
        } else {
            //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: trace point3");
            // Check if there is uncompressed data
            numBytes = uncompressedDirectBuf.remaining();
            if (numBytes > 0) {
                numBytes = Math.min(numBytes, len);
                //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: ????????????????? exist data in uncompressedDirectBuf numBytes is "+numBytes);
                ((ByteBuffer)uncompressedDirectBuf).get(b, off, numBytes);
                return numBytes;
            }
            //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor:  trace point4");
            // Check if there is data to decompress
            if (compressedDirectBufLen > 0) {
                // Re-initialize the lzo's output direct-buffer
                //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: trace point5");
                uncompressedDirectBuf.rewind();
                uncompressedDirectBuf.limit(directBufferSize);
                //uncompressedDirectBuf.limit(uncompressedBlockSize);

                // Decompress data
		//System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: try to call fpga decompressing...====>jni check directBufferSize is "+ directBufferSize);
		//System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: before decompress compressedDirectBufLen is "+compressedDirectBufLen+"  uncompressedDirectBuf.capacity() is "+uncompressedDirectBuf.capacity()+" uncompressedDirectBuf.remaining() is "+uncompressedDirectBuf.remaining());
                numBytes = decompressBytesDirect(uncompressedDirectBufLen);//native hook~~~~~~~~~~~~~~~~~~~~~~~
                //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: native decompressing finished compressedDirectBufLen is "+compressedDirectBufLen+" numBytes is "+numBytes+" <<<debug check");
		//pause(1);
	//        compressedDirectBufLen = 0;
                uncompressedDirectBuf.limit(numBytes);

                // Return atmost 'len' bytes
                numBytes = Math.min(numBytes, len);
                ((ByteBuffer)uncompressedDirectBuf).get(b, off, numBytes);
            }
        }

        // Set 'finished' if lzo has consumed all user-data
        if (userBufLen <= 0) {
            finished = true;
        }
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: >?????????? return from decompress() numBytes is "+numBytes);
        return numBytes;
    }
    public synchronized void setDictionary(byte[] b, int off, int len){
       //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: psudo setDictionary, used to pass uncompressedbuffSize "+len);
       uncompressedDirectBufLen = len;
    }
////////////////////////////un referenced function///////////////////////
    public synchronized boolean needsDictionary(){
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: needsDictionary()");
        return false;}
    public synchronized int getRemaining(){
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: getRemaining()");
        return userBufLen;}
    public synchronized void reset(){
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: reset()");
        finished = false;
        compressedDirectBufLen = 0;
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        userBufOff = userBufLen = 0;
    }
    public void end(){
    }
///////////////////////////////end///////////////////////////////////////
/**
   * Note whether the current block being decompressed is actually
   * stored as uncompressed data.  If it is, there is no need to 
   * use the lzo decompressor, and no need to update compressed
   * checksums.
   * 
   * @param uncompressed
   *          Whether the current block of data is uncompressed already.
   */
  public synchronized void setCurrentBlockUncompressed(boolean uncompressed) {
    //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: setCurrentBlockUncompressed");
    isCurrentBlockUncompressed = uncompressed;
  }
  /**
   * Reset all checksums registered for this decompressor instance.
   */
  public synchronized void resetChecksum() {
    //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: resetChecksum()");
    for (Checksum chk : chkDMap.values()) chk.reset();
    for (Checksum chk : chkCMap.values()) chk.reset();
  }

    protected synchronized boolean isCurrentBlockUncompressed() {
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: isCurrentBlockUncompressed()");
        return isCurrentBlockUncompressed;
    }

    public synchronized void setuncompressedDirectBufLen(int _uncompressedDirectBufLen) {
        //System.out.println("com.hadoop.compression.lzo.FpgaDecompressor: setuncompressedDirectBufLen is "+_uncompressedDirectBufLen);
        uncompressedDirectBufLen = _uncompressedDirectBufLen;
    }

    public void pause(long sleeptime) {
        long expectedtime = System.currentTimeMillis() + sleeptime*1000;
        while (System.currentTimeMillis() < expectedtime) {
        //Empty Loop   
        }
    }

    private synchronized static void initIDs(){
       __initIDs();
    }

    private synchronized int decompressBytesDirect(int uncompressedDirectBufLen){
       return __decompressBytesDirect(uncompressedDirectBufLen);
    }

    private synchronized void init(){
       __init();
    }
    private /*synchronized*/ native static void __initIDs();
    private /*synchronized*/ native void __init();
    //private native int decompressBytesDirect();//return numBytes of the decompressed files.
    private /*synchronized*/ native int __decompressBytesDirect(int uncompressedDirectBufLen);
//    private native int intMethod();
}
