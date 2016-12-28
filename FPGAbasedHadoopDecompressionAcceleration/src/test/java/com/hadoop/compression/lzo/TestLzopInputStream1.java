package com.hadoop.compression.lzo;

/**
 * Created by qianche1 on 1/27/2016.
 */

        import java.io.BufferedReader;
        import java.io.File;
        import java.io.FileInputStream;
        import java.io.IOException;
        import java.io.InputStreamReader;
        import java.nio.ByteBuffer;
        import java.security.NoSuchAlgorithmException;

        import junit.framework.TestCase;

        import org.apache.commons.logging.Log;
        import org.apache.commons.logging.LogFactory;
        //import org.apache.hadoop.io.compress.BlockDecompressorStream;
        import org.apache.hadoop.io.compress.Decompressor;
        import org.apache.hadoop.io.compress.DecompressorStream;
        import org.apache.hadoop.io.compress.BlockDecompressorStream;

/**
 * Test the LzopInputStream, making sure we get the same bytes when reading a compressed file through
 * it as when we read the corresponding uncompressed file through a standard input stream.
 */

public class TestLzopInputStream1 extends TestCase {
    private static final Log LOG = LogFactory.getLog(TestLzopInputStream.class);

    private String inputDataPath;

    // Filenames of various sizes to read in and verify.
    private final String bigFile = "100000.txt";
    private final String mediumFile = "1000.txt";
    private final String smallFile = "100.txt";
    private final String emptyFile = "0.txt";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        inputDataPath = System.getProperty("test.build.data", "data");
    }

    /**
     * Test against a 100,000 line file with multiple LZO blocks.
     */
    public void testBigFile() throws NoSuchAlgorithmException, IOException,
            InterruptedException {
        runTest(bigFile);
    }

    private void runTest(String filename) throws IOException,
            NoSuchAlgorithmException, InterruptedException {

        if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
            LOG.warn("Cannot run this test without the native lzo libraries");
            return;
        }

        // Assumes the flat file is at filename, and the compressed version is filename.lzo
        File textFile = new File(inputDataPath, filename);
        File lzoFile = new File(inputDataPath, filename + new LzopCodec().getDefaultExtension());
        LOG.info("Comparing files " + textFile + " and " + lzoFile);
        System.out.println("textFile is "+textFile+"\nlzoFile is "+lzoFile);
        // Set up the text file reader.
        BufferedReader textBr = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
        BufferedReader textBr0 = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
        // Set up the LZO reader.

        int lzoBufferSize = 256 * 1024;
        LzopDecompressor lzoDecompressor = new LzopDecompressor(lzoBufferSize);
        LzopInputStream lzoIn = new LzopInputStream(new FileInputStream(lzoFile.getAbsolutePath()), lzoDecompressor, lzoBufferSize);
        BufferedReader lzoBr = new BufferedReader(new InputStreamReader(lzoIn));

        FpgaDecompressor fpgaDecompressor = new FpgaDecompressor(lzoBufferSize);
        FileInputStream fis = new FileInputStream(lzoFile.getAbsolutePath());
        System.out.println("fis is "+fis);

        DecompressorStream ds = new DecompressorStream(fis,fpgaDecompressor,lzoBufferSize);
        ByteBuffer bb;
        bb = ByteBuffer.allocate(lzoBufferSize);
        int pos = bb.position();
        int lim = bb.limit();
        int rem = (pos <= lim ? lim - pos: 0);
        int rd = ds.read(bb.array(),bb.arrayOffset()+pos,rem);
        BlockDecompressorStream bds = new BlockDecompressorStream(fis,fpgaDecompressor);
        FpgaInputStream  fpgaIn = new FpgaInputStream(fis,fpgaDecompressor,lzoBufferSize);
        BufferedReader fpgaBr = new BufferedReader(new InputStreamReader(fpgaIn));

        // Now read line by line and compare.
        String textLine;
        String fpgaLine;
        int line = 0;
        while ((textLine = textBr.readLine()) != null) {
            line++;
            fpgaLine = fpgaBr.readLine();//<--this is where decompress actually triggered!
            if (!fpgaLine.equals(textLine)) {
                LOG.error("LZO decoding mismatch on line " + line + " of file " + filename);
                LOG.error("Text line: [" + textLine + "], which has length " + textLine.length());
                LOG.error("LZO line: [" + fpgaLine + "], which has length " + fpgaLine.length());
            }
            assertEquals(fpgaLine, textLine);
        }
        // Verify that the lzo file is also exhausted at this point.
        assertNull(fpgaBr.readLine());

        textBr.close();
        fpgaBr.close();
    }

}
