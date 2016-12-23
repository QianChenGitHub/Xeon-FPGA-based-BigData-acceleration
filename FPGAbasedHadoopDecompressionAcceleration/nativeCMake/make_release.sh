#!/bin/bash

export JAVA_HOME=/usr/lib/[*your_java_folder*]
#source /ipp/ipp/bin/ippvars.sh  intel64

rm -rf build CMakeCache.txt  CMakeFiles  Makefile  cmake_install.cmake

mkdir build

cd build
cmake .. -DCMAKE_CXX_COMPILER=g++ -DCMAKE_BUILD_TYPE=Release
#make -j4 install
make
cp *.so ../
res=$?
cd -

#cd build
#make
#cp *.so ../

#cp /home/charles/fpga/FpgaDecompressorNativeCompile/libFpgaDecompressor.so /home/charles/fpga/fpga-hadoop-lzo/fpgaCompile/target/native/Linux-amd64-64/lib/

#rm /home/charles/fpga/fpga-hadoop-lzo/fpga-lzo-release/target/native/Linux-amd64-64/lib/libFpgaDecompressor.so
#cp libFpgaDecompressor.so /home/charles/fpga/fpga-hadoop-lzo/fpga-lzo-release/target/native/Linux-amd64-64/lib/

exit $res
