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
            int itg=9;
            FpgaLibraryWrapper flw = new FpgaLibraryWrapper();
            int iii = flw.intMethod(itg);
            if(FpgaLibraryLoader.isFpgaLibraryLoaded()){
              //Initiate the FPGA library
              try{
                  initIDs();
                  FpgaLzoLoaded = true;
              } catch (Throwable t){
                  LOG.warn(t.toString());
                  FpgaLzoLoaded = false;
              }
            } else {
              LOG.error("Can't load FPGA LZO library!");
              FpgaLzoLoaded = false;
        }
    }
    //@Override
    public  FpgaDecompressor(int directBufferSize){
        this.directBufferSize = directBufferSize;
        //this.strategy = strategy;

        compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        init();//native hook~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
      // Consume remaining compressed data?
      if (uncompressedDirectBuf.remaining() > 0) {
        return false;
      }

      // Check if lzo has consumed all input
      if (compressedDirectBufLen <= 0) {
        // Check if we have consumed all user-input
        if (userBufLen <= 0) {
          return true;
        } else {
          setInputFromSavedData();
        }
      }

      return false;
    }

    public synchronized boolean finished() {
        // Check if 'lzo' says its 'finished' and
        // all uncompressed data has been consumed
        return (finished && uncompressedDirectBuf.remaining() == 0);
    }
    public synchronized int decompress(byte[] b, int off, int len)
            throws IOException {
	     if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        int numBytes = 0;
        if (isCurrentBlockUncompressed()) {
            // The current block has been stored uncompressed, so just
            // copy directly from the input buffer.
            numBytes = Math.min(userBufLen, len);
            System.arraycopy(userBuf, userBufOff, b, off, numBytes);
            userBufOff += numBytes;
            userBufLen -= numBytes;
        } else {
            // Check if there is uncompressed data
            numBytes = uncompressedDirectBuf.remaining();
            if (numBytes > 0) {
                numBytes = Math.min(numBytes, len);
                ((ByteBuffer)uncompressedDirectBuf).get(b, off, numBytes);
                return numBytes;
            }
            // Check if there is data to decompress
            if (compressedDirectBufLen > 0) {
                // Re-initialize the lzo's output direct-buffer
                uncompressedDirectBuf.rewind();
                uncompressedDirectBuf.limit(directBufferSize);
                //uncompressedDirectBuf.limit(uncompressedBlockSize);

                // Decompress data
                numBytes = decompressBytesDirect(uncompressedDirectBufLen);//native hook~~~~~~~~~~~~~~~~~~~~~~~
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
        return numBytes;
    }
    public synchronized void setDictionary(byte[] b, int off, int len){
       uncompressedDirectBufLen = len;
    }
////////////////////////////un referenced function///////////////////////
    public synchronized boolean needsDictionary(){
        return false;}
    public synchronized int getRemaining(){
        return userBufLen;}
    public synchronized void reset(){
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
    private /*synchronized*/ native int __decompressBytesDirect(int uncompressedDirectBufLen);

}
