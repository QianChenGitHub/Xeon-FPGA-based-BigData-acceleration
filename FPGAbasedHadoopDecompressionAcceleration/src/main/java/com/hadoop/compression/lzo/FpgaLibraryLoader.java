package com.hadoop.compression.lzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Chen Qian on 1/20/2016.
 * intel Corp.
 */
 
public class FpgaLibraryLoader {

    private static final Log LOG = LogFactory.getLog(FpgaLibraryLoader.class);
    private static boolean fpgaLibraryLoaded = false;

    static {
        try {
            //try to load the lib
            System.loadLibrary("fpgadecompress");
            fpgaLibraryLoaded = true;
            LOG.info("Loaded fpga decompress library");
        } catch (Throwable t) {
            LOG.error("Could not load fpga decompress library", t);
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
