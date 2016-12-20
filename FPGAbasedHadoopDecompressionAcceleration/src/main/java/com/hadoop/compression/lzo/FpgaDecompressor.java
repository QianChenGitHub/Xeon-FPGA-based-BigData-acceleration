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

//import org.apache.hadoop.io.compress.Decompressor;

public class FpgaDecompressor extends DecompressorAbstract{
    private final EnumMap<DChecksum,Checksum> chkDMap = new EnumMap<DChecksum,Checksum>(DChecksum.class);
    private final EnumMap<CChecksum,Checksum> chkCMap = new EnumMap<CChecksum,Checksum>(CChecksum.class);
    private static final Log LOG = LogFactory.getLog(LzoDecompressor.class.getName());
    private static boolean FpgaLzoLoaded;
    private int directBufferSize;
    private Buffer compressedDirectBuf = null;
    private int compressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finished;

    private long fpgaDecompressor = 0;

    // Whether or not the current block being is actually stored uncompressed.
    // This happens when compressing a block would increase it size.
    private boolean isCurrentBlockUncompressed;

    static {
        if(FpgaLibraryLoader.isFpgaLibraryLoaded()){
            //Initiate the FPGA library
            try{
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

    public FpgaDecompressor(int directBufferSize){
        System.out.print("hello fpga lzo demo!");
        this.directBufferSize = directBufferSize;
        //this.strategy = strategy;

        compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        //init(this.getDecompressor());//native hook~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    }

    public static boolean isFpgaLzoLoaded(){
        return FpgaLzoLoaded;
    }

    public void initHeaderFlags(EnumSet<DChecksum> dflags,
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

                // Decompress data
                numBytes = decompressBytesDirect(strategy.getDecompressor());//native hook~~~~~~~~~~~~~~~~~~~~~~~
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

    protected synchronized boolean isCurrentBlockUncompressed() {
        return isCurrentBlockUncompressed;
    }
    private native void init();
    private native int decompressBytesDirect();//return numBytes of the decompressed files.
}
