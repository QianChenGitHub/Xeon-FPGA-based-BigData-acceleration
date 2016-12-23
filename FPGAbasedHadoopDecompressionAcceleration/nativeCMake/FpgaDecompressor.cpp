#include "com_hadoop_compression_lzo_FpgaDecompressor.h"

//#include "gpl-compression.h"
#include <stdio.h>
#include <stdlib.h>
#include "tools.h"
#include "lzo_fpga.h"

//#define INIT_LOAD
#define LOG
#define LZO_INPUT_STREAM
#define FUNCPTR
#ifdef FUNCPTR
#include "error.h"
#include <ltdl.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <string.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>

#include <unistd.h>

typedef int32_t API_CALL_CONVENTION (* device_init_t)(device_description_t *const description);
typedef int32_t API_CALL_CONVENTION (* device_interface_load_t)(uint32_t version, void *const interface);
typedef int32_t API_CALL_CONVENTION (* device_unload_t)();


device_status_t encode(std::vector<workload_t> &workloads);
device_status_t decode(std::vector<workload_t> &workloads);
int get_core_number();
device_interface_t            device_interface;
std::string                   compression_method;
bool load_library(std::string lib_name);
void unload_library();
device_init_t                 device_init_i;
device_interface_load_t       device_interface_load_i;
device_unload_t               device_unload_i;
int                           core_number = -1;
lt_dlhandle                   library_handle;


bool load_library(std::string lib_name)
{
  lt_dlinit();
#ifdef LOG
  print_status("Looking for umd driver in %s\n", lib_name.c_str());
#endif
  library_handle = lt_dlopen(lib_name.c_str());
  if (library_handle == nullptr) {
    print_status("Error loading driver: %s\n", lt_dlerror());
    return false;
  }

  // load symbols
  device_init_i = reinterpret_cast<device_init_t>(lt_dlsym(library_handle, "device_init"));
  device_interface_load_i =
      reinterpret_cast<device_interface_load_t>(lt_dlsym(library_handle, "device_interface_load"));
  device_unload_i = reinterpret_cast<device_unload_t>(lt_dlsym(library_handle, "device_unload"));

  // perform device load
  device_description_t description;
  if (device_init_i(&description)) {
#ifdef LOG
    print_status("Failed to load device ...\n");
#endif
    return false;
  }
#ifdef LOG
  std::cout << "Device: " << description.version_first << "\n"
            << "Version: " << description.version_last << "\n"
            << "Name: " << description.name << "\n"
            << "Description: " << description.description << std::endl;
#endif
  int result = sscanf(description.description, "The FPGA contains %d cores", &core_number);
  if (result != 1)
    throw std::runtime_error("Couldn't decode core count from device description");

  // load interface functions
  if (device_interface_load_i(0, &device_interface)) {
#ifdef LOG
    print_status("Failed to open interface ...\n");
#endif
    return false;
  }
#ifdef LOG
  print_status("^^^^^^^^^^^^^^^^^^^^^^^^^^^^load finished^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
#endif
  return true;
}


void unload_library()
{
  if (device_unload)
    device_unload_i();

  if (library_handle)
    lt_dlclose(library_handle);
#ifdef LOG
  print_status("Unload finished\n");
#endif
}


int get_core_number()
{
  return core_number;
}

device_status_t decode(std::vector<workload_t> &workloads)
{
//  std::cout << "call device_interface.lzo_decompress interface to decompression" << std::endl;
  return device_interface.lzo_decompress(workloads.data(), (uint32_t)workloads.size());
};
#endif

void throwJavaException(JNIEnv *env)
{
    // You can put your own exception here
//    jclass c = env->FindClass("java/lang/RuntimeException");
//    if (NULL == c)
//    {
        //B plan: null pointer ...
//        c = env->FindClass("java/lang/NullPointerException");
//    }
//    env->ThrowNew(c, msg);
    try{
        //std::cout<<"@@@@@@@@@@@@@@@check through@@@@@@@@@@"<<std::endl;
	throw;
    }
    catch(const std::bad_alloc& e){
        jclass jc = env->FindClass("java/lang/OutOfMemoryError");
        if(jc) env->ThrowNew (jc, e.what());
    }
    catch (const std::ios_base::failure& e) {
        jclass jc = env->FindClass("java/io/IOException");
        if(jc) env->ThrowNew (jc, e.what()); 
    }
    catch (const std::exception& e) {
        jclass jc = env->FindClass("java/lang/Error");
        if(jc) env->ThrowNew (jc, e.what());
    }
    catch (...) {
        jclass jc = env->FindClass("java/lang/Error");
        if(jc) env->ThrowNew (jc, "Unidentified exception => "
                    "rethrow_cpp_exception_as_java_exception() "
                    "may require some completion..." );
    }
}


#define JLONG(func_ptr) ((jlong)((ptrdiff_t)(func_ptr)))

#define LOCK_CLASS(env, clazz, classname) \
  if (env->MonitorEnter(clazz) != 0) { \
    char exception_msg[128]; \
    printf("Failed to lock %s \n",classname) ; \
  }

#define UNLOCK_CLASS(env, clazz, classname) \
  if (env->MonitorExit(clazz) != 0) { \
    char exception_msg[128]; \
    printf("Failed to unlock %s \n",classname); \
  }

/* A helper macro to 'throw' a java exception. */ 
#define THROW(env, exception_name, message) \
  { \
	jclass ecls = env->FindClass(exception_name); \
	if (ecls) { \
	  env->ThrowNew(ecls, message); \
	  env->DeleteLocalRef(ecls); \
	} \
  }

static jfieldID FpgaDecompressor_clazz;
static jfieldID FpgaDecompressor_compressedDirectBuf;
static jfieldID FpgaDecompressor_compressedDirectBufLen;
static jfieldID FpgaDecompressor_uncompressedDirectBuf;
static jfieldID FpgaDecompressor_directBufferSize;
static jfieldID FpgaDecompressor_fpgaDecompressor;
//static jfieldID FpgaDecompressor_uncompressedBlockSizeJNI;
static jfieldID FpgaDecompressor_FpgaEnc;

//LzoEngineAcc lzo_fpga("empty");
JNIEXPORT void JNICALL Java_com_hadoop_compression_lzo_FpgaDecompressor__1_1initIDs(
  JNIEnv *env, jclass _class
  ){
  printf("com_hadoop_compression_lzo_FpgaDecompressor_initIDs: initIDs \n");
  FpgaDecompressor_clazz = env->GetStaticFieldID(_class, "clazz", 
                                                   "Ljava/lang/Class;");
  FpgaDecompressor_FpgaEnc = env->GetStaticFieldID(_class,"FpgaEnc",
                                                   "Ljava/lang/Class;");
//  LzoDecompressor_finished = (*env)->GetFieldID(env, class, "finished", "Z");
  FpgaDecompressor_compressedDirectBuf = env->GetFieldID(_class, 
                                                "compressedDirectBuf", 
                                                "Ljava/nio/Buffer;");
  FpgaDecompressor_compressedDirectBufLen = env->GetFieldID(_class, 
                                                    "compressedDirectBufLen", "I");
  FpgaDecompressor_uncompressedDirectBuf = env->GetFieldID(_class, 
                                                  "uncompressedDirectBuf", 
                                                  "Ljava/nio/Buffer;");
  FpgaDecompressor_directBufferSize = env->GetFieldID(_class, 
                                              "directBufferSize", "I");
  FpgaDecompressor_fpgaDecompressor = env->GetFieldID(_class,
                                              "fpgaDecompressor", "J");
//  FpgaDecompressor_uncompressedBlockSizeJNI = env->GetFieldID(_class,
//                                              "uncompressedBlockSizeJNI", "I");
//  LzoDecompressor_lzoDecompressor = (*env)->GetFieldID(env, class,
//                                              "lzoDecompressor", "J");  
//  printf("com_hadoop_compression_lzo_FpgaDecompressor_initIDs: finished ID init now loading lib\n");
//  if (!load_library("libumd_lzo.so"))
//     throw std::runtime_error("Loading library failed ...");
//  printf("com_hadoop_compression_lzo_FpgaDecompressor_initIDs: load_library loaded finished\n");
}


JNIEXPORT void JNICALL
Java_com_hadoop_compression_lzo_FpgaDecompressor__1_1init(
  JNIEnv *env, jobject thisObj
  ) {
  // Initialize the lzo library
   printf("com_hadoop_compression_lzo_FpgaDecompressor_init: enter __init()\n");
#ifdef FUNCPTR
#ifdef INIT_LOAD
  if (!load_library("libumd_lzo.so"))
    throw std::runtime_error("Loading library failed ...");
#endif
  core_number = get_core_number();
#else
  LzoEngineAcc lzo_fpga("empty");
#endif
  device_status_t (*fptr)(std::vector<workload_t>&) ;
  fptr = &decode;
  env->SetLongField(thisObj,FpgaDecompressor_fpgaDecompressor,JLONG(fptr));
  printf("com_hadoop_compression_lzo_FpgaDecompressor_init: finished init()\n");
  return;
}

JNIEXPORT jint JNICALL
//Java_com_hadoop_compression_lzo_FpgaDecompressor_decompressBytesDirect(
Java_com_hadoop_compression_lzo_FpgaDecompressor__1_1decompressBytesDirect(
	JNIEnv *env, jobject thisObj, jint uncompressedDirectBufLen
	) {
     try{ 
        //try cache.
#ifndef FUNCPTR
        LzoEngineAcc lzo_fpga("empty");
#endif
//	std::cout<<"Java_com_hadoop_compression_lzo_FpgaDecompressor_decompressBytesDirect: entered"<<std::endl;
#ifndef INIT_LOAD
        if (!load_library("libumd_lzo.so"))
           throw std::runtime_error("Loading library failed ...");
#endif
	jclass clazz;
        jobject obj = env->GetStaticObjectField(clazz,FpgaDecompressor_clazz);
	jobject compressed_direct_buf = env->GetObjectField(thisObj,FpgaDecompressor_compressedDirectBuf);
	uint32_t compressed_direct_buf_len = env->GetIntField(thisObj,FpgaDecompressor_compressedDirectBufLen);
	jobject uncompressed_direct_buf = env->GetObjectField(thisObj,FpgaDecompressor_uncompressedDirectBuf);
	uint32_t uncompressed_direct_buf_len = env->GetIntField(thisObj,FpgaDecompressor_directBufferSize);
        jlong fpga_decompressor_funcptr = env->GetLongField(thisObj,
                                              FpgaDecompressor_fpgaDecompressor);
        int mnt=0;
        if ((mnt = env->MonitorEnter(obj)) != 0) 
            std::cout << "lock clazz failed with id " <<mnt<< std::endl;
	void* uncompressed_bytes = env->GetDirectBufferAddress(uncompressed_direct_buf);
        if (env->MonitorExit(obj) != 0)
            std::cout << "unlock clazz failed" << std::endl; 

 	if (uncompressed_bytes == 0) {
            return (jint)0;
	}
       if (env->MonitorEnter(obj) != 0)
           std::cout << "lock clazz failed" << std::endl;
       void* compressed_bytes = env->GetDirectBufferAddress(compressed_direct_buf);
        if (env->MonitorExit(obj) != 0)
            std::cout << "unlock clazz failed" << std::endl;
        if (compressed_bytes == 0) {
	    return (jint)0;
	}
#ifdef  LZO_INPUT_STREAM
        uint32_t no_uncompressed_bytes = uncompressedDirectBufLen;//uncompressed_direct_buf_len;
#else
        uint32_t no_uncompressed_bytes = uncompressed_direct_buf_len;
#endif
        std::cout<<"uncompressedDirectBufLen is "<<uncompressedDirectBufLen<<" uncompressed_direct_buf_len is "<<uncompressed_direct_buf_len<<std::endl;
        workload_t workload;
        workload.input_buffer           = compressed_bytes;
        workload.input_buffer_bytes     = compressed_direct_buf_len;
        workload.output_buffer          = uncompressed_bytes;//should input to decoder
        workload.output_buffer_bytes    = no_uncompressed_bytes;//not out put from decoder, its read from compressed file block header.
        workload.status                 = WORKLOAD_OK;
        workload.measurement            = 0;
        workload.payload_mode           = 0;
        workload.compress_mode		= 0;
        std::vector<workload_t> workloads;
        workloads.clear();
        workloads.push_back(workload);//issue exist here!
#ifdef FUNCPTR
#ifdef LOG
        printf("**********************************before decompress************************************\n");
        printf("workload.input_buffer is \t %u \n",workload.input_buffer);
        printf("workload.input_buffer_bytes is \t %u \n",workload.input_buffer_bytes);
        printf("workload.output_buffer is \t %u \n",workload.output_buffer);
        printf("workload.output_buffer_bytes is  %u \n",workload.output_buffer_bytes);
        printf("workload.status is \t %d \n ",workload.status);
        printf("workload.measurement is \t %d \n",workload.measurement);
        printf("workload.payload_mode is \t %d \n",workload.payload_mode);
        printf("**********************************static finished************************************\n");
#endif
        auto status = decode(workloads);
#ifdef LOG     
        printf("**********************************after decompress************************************\n");
        printf("workload.input_buffer is \t %u \n",workload.input_buffer);
        printf("workload.input_buffer_bytes is \t %u \n",workload.input_buffer_bytes);
        printf("workload.output_buffer is \t %u \n",workload.output_buffer);
        printf("workload.output_buffer_bytes is \t %u \n",workload.output_buffer_bytes);
        printf("workload.status is \t %d \n ",workload.status);
        printf("workload.measurement is \t %d \n",workload.measurement);
        printf("workload.payload_mode is \t %d \n",workload.payload_mode);
        printf("**********************************static finished************************************\n");
#endif
#else
	auto status = lzo_fpga.decode(workloads);
#endif
//#define DBG
#ifdef DBG
    std::string out_file_name = "out/decompressedBuff.txt";
    std::cout << out_file_name << std::endl;
    //printf("com_hadoop_compression_lzo_FpgaDecompressor: out_file_name is %s uncompressed_bytes.size() is %d\n",out_file_name.c_str(),uncompressed_bytes.length());
    FILE *out_file = fopen(out_file_name.c_str(), "wb");
    if (!out_file)
      throw std::runtime_error("Cannot write output file");
    auto bytes =
        fwrite(workload.output_buffer, 1, workload.output_buffer_bytes , out_file);
    fclose(out_file);
#endif
        for (auto &workload : workloads) {
             std::cout << "clean up workload"<<std::endl;
             //cleanup_workload(workload);
        }
        workloads.clear();
        if (status == DEV_OK) {
//          std::cout << "decode finished" <<std::endl;
          env->SetIntField(thisObj, FpgaDecompressor_compressedDirectBufLen, 0);
        } else {
          std::cout<<"decode failed"<<std::endl;
          const int msg_len = 1024;
          char exception_msg[msg_len];
          snprintf(exception_msg, msg_len, "%s returned: %d",
              "fpga decode error", status);
          THROW(env, "java/lang/InternalError", exception_msg);
        }

#ifdef FUNCPTR
//       if(workload.output_buffer_bytes < 60000){
//        std::cout<<"workload.output_buffer_bytes is "<<workload.output_buffer_bytes<<std::endl;
#ifndef INIT_LOAD
        unload_library();
#else
        std::cout<<"here uncompressedDirectBufLen is "<<uncompressedDirectBufLen<<" uncompressed_direct_buf_len is "<<uncompressed_direct_buf_len<<std::endl;
        if(no_uncompressed_bytes < (uncompressed_direct_buf_len * 0.9)){
            std::cout << "unload_library"<<std::endl;
            unload_library();
        }
#endif
//       }
#endif
//        pause(); 
//        sleep(1);
        int ret = no_uncompressed_bytes;
#ifdef LOG
        std::cout << "lzo finished decoding now return from jni "<<std::endl;
#endif
   }
   catch(...)
   {
      std::cout << "@@@@@@@@@check catch@@@@@@@@@@@@@@@@@@@@@@"<<std::endl;
      throwJavaException(env);
   }
   return uncompressedDirectBufLen;
}

