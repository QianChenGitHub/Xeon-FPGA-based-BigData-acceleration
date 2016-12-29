# Xeon-FPGA-based-BigData-acceleration
This repo is create to share FPGA based BigData Acceleration project.
Current folder is our first project on BigData Acceleration work, which contains source code to integrate FPGA lzo decompression function into hadoop systems.
Only software source code and library binaries are put into the related folders, if you need the lzo decompression bit stream used for programing the FPGA please contact me!
Documentations would be add on demand, any question about this project please feel free to contact me!

before build the source code you need to install lzo library fisrt:

Download the latest LZO release from http://www.oberhumer.com/opensource/lzo/
Configure LZO to build a shared library (required) and use a package-specific prefix (optional but recommended): ./configure --enable-shared --prefix /usr/local/lzo-2.06
Build and install LZO: make && sudo make install


Quick Start Compile Scripts:

You can use "sh make" command at project root path to quick compile and run project test.

Compile steps:

1. generate jni header file by maven.

2. put jni header into CMake native compiling folder and then compile native code to generate share object: "libFpgaDecompressor.so"

3. copy "libFpgaDecompressor.so" into ./target/native/Linux-amd64-64/lib/ folder.

4. run test command: 
    'C_INCLUDE_PATH=/usr/local/lzo-2.06/include \
      LIBRARY_PATH=/usr/local/lzo-2.06/lib \ 
      mvn test'

5. run "mvn package" again to generate the target jar package. 

6. deploy generated binaries into Hadoop paths.

About native code compile:
CMake was used to compile the native code of this project for the following reasons:

1. FPGA accelerator api and related librarires are developed in CMake environment, easier to develop native weapper in CMake.   

2. CMake integrated into maven compiler was not well support during the developing time of this project.
