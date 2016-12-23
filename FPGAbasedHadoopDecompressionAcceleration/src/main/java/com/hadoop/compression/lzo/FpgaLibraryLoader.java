/**
 * Created by Chen Qian on 1/20/2016.
 * intel Corp.
 */
 
package com.hadoop.compression.lzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FpgaLibraryLoader {

    private static final Log LOG = LogFactory.getLog(FpgaLibraryLoader.class);
    private static boolean fpgaLibraryLoaded = false;
    static {
        try {
            //try to load the lib
            System.out.println("com.hadoop.compression.lzo.FpgaLibraryLoader: loader ~~");
            System.loadLibrary("FpgaDecompressor");
	    //System.loadLibrary("NativeValidation");
            
            fpgaLibraryLoaded = true;
            System.out.println("com.hadoop.compression.lzo.FpgaLibraryLoader: loader success");
            LOG.info("com.hadoop.compression.lzo.FpgaLibraryLoader: Loaded fpga decompress library");
        } catch (Throwable t) {
            System.out.println("com.hadoop.compression.lzo.FpgaLibraryLoader: loader failed");
            LOG.error("com.hadoop.compression.lzo.FpgaLibraryLoader: Could not load fpga decompress library", t);
            fpgaLibraryLoaded = false;
        }
    }

    /**
     * Are the native gpl libraries loaded?
     * @return true if loaded, otherwise false
     */
    public static boolean isFpgaLibraryLoaded() {
        return fpgaLibraryLoaded;
    }
}
