package com.hadoop.compression.lzo;

/**
 * Created by Chen Qian on 1/21/2016.
 * intel Corp.
 */

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.BlockDecompressorStream;

import org.apache.hadoop.io.compress.Decompressor;

public class FpgaInputStream extends BlockDecompressorStream{
    private static final Log LOG = LogFactory.getLog(FpgaInputStream.class);

    private final EnumSet<DChecksum> dflags = EnumSet.allOf(DChecksum.class);
    private final EnumSet<CChecksum> cflags = EnumSet.allOf(CChecksum.class);

    private final byte[] buf = new byte[9];

    private final EnumMap<DChecksum,Integer> dcheck = new EnumMap<DChecksum,Integer>(DChecksum.class);
    private final EnumMap<CChecksum,Integer> ccheck = new EnumMap<CChecksum,Integer>(CChecksum.class);

    private int noUncompressedBytes = 0;
    private int noCompressedBytes = 0;
    private int uncompressedBlockSize = 0;

    public FpgaInputStream(InputStream in, Decompressor decompressor, int bufferSize) throws IOException{
        super(in,decompressor,bufferSize);
        readHeader(in);
    }

    private static void readFully( InputStream in, byte buf[],
                                   int off, int len ) throws IOException, EOFException {
        int toRead = len;
        while ( toRead > 0 ) {
            int ret = in.read( buf, off, toRead );
            if ( ret < 0 ) {
                throw new EOFException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    private static int readInt(InputStream in, byte[] buf, int len)
            throws IOException {
        readFully(in, buf, 0, len);
        int ret = (0xFF & buf[0]) << 24;
        ret    |= (0xFF & buf[1]) << 16;
        ret    |= (0xFF & buf[2]) << 8;
        ret    |= (0xFF & buf[3]);
        return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }


    private static int readHeaderItem(InputStream in, byte[] buf, int len,
                                      Adler32 adler, CRC32 crc32) throws IOException {
        int ret = readInt(in, buf, len);
        adler.update(buf, 0, len);
        crc32.update(buf, 0, len);
        Arrays.fill(buf, (byte)0);
        return ret;
    }

    protected void readHeader(InputStream in) throws IOException {
        readFully(in, buf, 0, 9);
        if (!Arrays.equals(buf, LzopCodec.LZO_MAGIC)) {
            throw new IOException("Invalid LZO header");
        }
        Arrays.fill(buf, (byte)0);
        Adler32 adler = new Adler32();
        CRC32 crc32 = new CRC32();
        int hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop version
        if (hitem > LzopCodec.LZOP_VERSION) {
            LOG.debug("Compressed with later version of lzop: " +
                    Integer.toHexString(hitem) + " (expected 0x" +
                    Integer.toHexString(LzopCodec.LZOP_VERSION) + ")");
        }
        hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzo library version
        if (hitem < LzoDecompressor.MINIMUM_LZO_VERSION) {
            throw new IOException("Compressed with incompatible lzo version: 0x" +
                    Integer.toHexString(hitem) + " (expected at least 0x" +
                    Integer.toHexString(LzoDecompressor.MINIMUM_LZO_VERSION) + ")");
        }
        hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop extract version
        if (hitem > LzopCodec.LZOP_VERSION) {
            throw new IOException("Compressed with incompatible lzop version: 0x" +
                    Integer.toHexString(hitem) + " (expected 0x" +
                    Integer.toHexString(LzopCodec.LZOP_VERSION) + ")");
        }
        hitem = readHeaderItem(in, buf, 1, adler, crc32); // method
        if (hitem < 1 || hitem > 3) {
            throw new IOException("Invalid strategy: " +
                    Integer.toHexString(hitem));
        }
        readHeaderItem(in, buf, 1, adler, crc32); // ignore level

        // flags
        hitem = readHeaderItem(in, buf, 4, adler, crc32);
        try {
            for (DChecksum f : dflags) {
                if (0 == (f.getHeaderMask() & hitem)) {
                    dflags.remove(f);
                } else {
                    dcheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
                }
            }
            for (CChecksum f : cflags) {
                if (0 == (f.getHeaderMask() & hitem)) {
                    cflags.remove(f);
                } else {
                    ccheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
                }
            }
        } catch (InstantiationException e) {
            throw new RuntimeException("Internal error", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Internal error", e);
        }
        ((FpgaDecompressor)decompressor).initHeaderFlags(dflags, cflags);
        boolean useCRC32 = 0 != (hitem & 0x00001000);   // F_H_CRC32
        boolean extraField = 0 != (hitem & 0x00000040); // F_H_EXTRA_FIELD
        if (0 != (hitem & 0x400)) {                     // F_MULTIPART
            throw new IOException("Multipart lzop not supported");
        }
        if (0 != (hitem & 0x800)) {                     // F_H_FILTER
            throw new IOException("lzop filter not supported");
        }
        if (0 != (hitem & 0x000FC000)) {                // F_RESERVED
            throw new IOException("Unknown flags in header");
        }
        // known !F_H_FILTER, so no optional block

        readHeaderItem(in, buf, 4, adler, crc32); // ignore mode
        readHeaderItem(in, buf, 4, adler, crc32); // ignore mtime
        readHeaderItem(in, buf, 4, adler, crc32); // ignore gmtdiff
        hitem = readHeaderItem(in, buf, 1, adler, crc32); // fn len
        if (hitem > 0) {
            // skip filename
            int filenameLen = Math.max(4, hitem); // buffer must be at least 4 bytes for readHeaderItem to work.
            readHeaderItem(in, new byte[filenameLen], hitem, adler, crc32);
        }
        int checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
        hitem = readHeaderItem(in, buf, 4, adler, crc32); // read checksum
        if (hitem != checksum) {
            throw new IOException("Invalid header checksum: " +
                    Long.toHexString(checksum) + " (expected 0x" +
                    Integer.toHexString(hitem) + ")");
        }
        if (extraField) { // lzop 1.08 ultimately ignores this
            LOG.debug("Extra header field not processed");
            adler.reset();
            crc32.reset();
            hitem = readHeaderItem(in, buf, 4, adler, crc32);
            readHeaderItem(in, new byte[hitem], hitem, adler, crc32);
            checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
            if (checksum != readHeaderItem(in, buf, 4, adler, crc32)) {
                throw new IOException("Invalid checksum for extra header field");
            }
        }
    }

}
