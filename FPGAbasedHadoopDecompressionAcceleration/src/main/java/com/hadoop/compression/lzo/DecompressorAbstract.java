package com.hadoop.compression.lzo;

/**
 * Created by qianche1 on 1/25/2016.
 */

import java.util.EnumMap;
import java.util.EnumSet;
import org.apache.hadoop.io.compress.Decompressor;

public abstract class DecompressorAbstract implements Decompressor{

    public abstract void setInput(byte[] b, int off, int len);
    public abstract boolean needsInput();
    public abstract void setDictionary(byte[] b, int off, int len);
    public abstract boolean needsDictionary();
    public abstract boolean finished();
    public abstract int getRemaining();
    public abstract void reset();
    public abstract void end();

    //public abstract void FpgaDecompressor(int directBufferSize);
    public abstract boolean isFpgaLzoLoaded();
    public abstract void initHeaderFlags(EnumSet<DChecksum> dflags,
                                         EnumSet<CChecksum> cflags);
    public abstract int decompress(byte[] b, int off, int len);
    protected abstract  boolean isCurrentBlockUncompressed();
    protected abstract void init();
    protected abstract int decompressBytesDirect();
}


