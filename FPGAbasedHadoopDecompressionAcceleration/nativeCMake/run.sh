#!/bin/bash
SCALA_LIB_HOME=/usr/share/java/
SCALA_CP=$SCALA_LIB_HOME/scala-library.jar:$SCALA_LIB_HOME/scala-reflect.jar
#scala -Djava.library.path=$(pwd) -cp . deCompressTestObject $1 
rm -rf ./out
mkdir out
scala -Djava.library.path=$(pwd) -cp . deCompressTestObject ../project/lzo_decompress/iom2.5_lzo1.5-20151120_ww47/testcases

