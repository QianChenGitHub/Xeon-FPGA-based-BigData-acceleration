#include "com_hadoop_compression_lzo_FpgaDecompressor.h"

#include <stdio.h>
#include <stdlib.h>
#include "tools.h"
#include "lzo_fpga.h"

#define JLONG(func_ptr) ((jlong)((ptrdiff_t)(func_ptr)))

#define LOCK_CLASS(env, clazz, classname) \
  if (env->MonitorEnter(clazz) != 0) { \
    char exception_msg[128]; \
    snprintf(exception_msg, 128, "Failed to lock %s", classname); \ 
  }

#define UNLOCK_CLASS(env, clazz, classname) \
  if (env->MonitorExit(clazz) != 0) { \
    char exception_msg[128]; \
    snprintf(exception_msg, 128, "Failed to unlock %s", classname); \ 
  }

static jfieldID FpgaDecompressor_clazz;
static jfieldID FpgaDecompressor_compressedDirectBuf;
static jfieldID FpgaDecompressor_compressedDirectBufLen;
static jfieldID FpgaDecompressor_uncompressedDirectBuf;
static jfieldID FpgaDecompressor_directBufferSize;
static jfieldID FpgaDecompressor_fpgaDecompressor;

LzoEngineAcc lzo_fpga("empty");

JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_FpgaDecompressor_init(
  JNIEnv *env, jobject thisObj
  ) {
  // Initialize the lzo library
  //LzoEngineAcc lzo_fpga("empty");

  // Save the decompressor-function into LzoDecompressor_lzoDecompressor
  env->SetLongField(thisObj,FpgaDecompressor_fpgaDecompressor,JLONG(&lzo_fpga));
                        //JLONG(&lzo_fpga));
  return;
}

JNIEXPORT jint JNICALL
Java_com_hadoop_compression_lzo_FpgaDecompressor_decompressBytesDirect(
	JNIEnv *env, jobject thisObj, jint decompressor
	) {
  //const char *lzo_decompressor_function = lzo_decompressors[decompressor];

	// Get members of LzoDecompressor
	jclass clazz ; 
        env->GetStaticObjectField(clazz,FpgaDecompressor_clazz);
	jobject compressed_direct_buf = env->GetObjectField(thisObj,FpgaDecompressor_compressedDirectBuf);
	uint32_t compressed_direct_buf_len = env->GetIntField(thisObj,FpgaDecompressor_compressedDirectBufLen);

	jobject uncompressed_direct_buf = env->GetObjectField(thisObj,FpgaDecompressor_uncompressedDirectBuf);
	uint32_t uncompressed_direct_buf_len = env->GetIntField(thisObj,FpgaDecompressor_directBufferSize);

  //jlong lzo_decompressor_funcptr = (*env)->GetLongField(env, this,
  //                                            FpgaDecompressor_lzoDecompressor);

    // Get the input direct buffer
	LOCK_CLASS(env, clazz, "FpgaDecompressor");
	    void* uncompressed_bytes = env->GetDirectBufferAddress(uncompressed_direct_buf);
        UNLOCK_CLASS(env, clazz, "FpgaDecompressor");

 	if (uncompressed_bytes == 0) {
            return (jint)0;
	}

    // Get the output direct buffer
        LOCK_CLASS(env, clazz, "FpgaDecompressor");
	    void* compressed_bytes = env->GetDirectBufferAddress(compressed_direct_buf);
        UNLOCK_CLASS(env, clazz, "FpgaDecompressor");

        if (compressed_bytes == 0) {
	    return (jint)0;
	}

        uint32_t no_uncompressed_bytes = uncompressed_direct_buf_len;

	// Decompress
/*
  lzo_decompress_t fptr = (lzo_decompress_t) FUNC_PTR(lzo_decompressor_funcptr);
	int rv = fptr(              \
	compressed_bytes,           \
	compressed_direct_buf_len,  \
	uncompressed_bytes,         \
	&no_uncompressed_bytes,     \
    NULL); //<--this is where actual decompression happen.
*/
/************************************FPGA LZO decompression*****************************************/
        std::vector<workload_t> workloads;
        workload_t workload;
        workload.input_buffer           = compressed_bytes;
        workload.input_buffer_bytes     = compressed_direct_buf_len;
        workload.output_buffer          = uncompressed_bytes;//should input to decoder
        workload.output_buffer_bytes    = no_uncompressed_bytes;//out put from decoder
        workload.status                 = WORKLOAD_OK;
        workload.measurement            = 0;
        workload.payload_mode           = 1;
        workloads.push_back(workload);
        auto status = lzo_fpga.decode(workloads);

        if (status == DEV_OK) {
    // lzo decompresses all input data
          env->SetIntField(thisObj, FpgaDecompressor_compressedDirectBufLen, 0);
        } else {
          const int msg_len = 1024;
          char exception_msg[msg_len];
          snprintf(exception_msg, msg_len, "%s returned: %d",
              "fpga decode error", status);
    //THROW(env, "java/lang/InternalError", exception_msg);
        }

        return no_uncompressed_bytes;
}

/**
 * vim: sw=2: ts=2: et:
 */
