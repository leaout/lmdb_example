#!/bin/bash

if [ $# != 1 ] ; then
  echo "USAGE: $0 release|debug"
  echo " e.g.: $0 debug"
  exit 1;
fi


CPU_CORES=4

#build protos
SCRIPT_PATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
cd $SCRIPT_PATH
GRPC_CPP_PLUGIN_PATH="$(which grpc_cpp_plugin)"

# build
#create dir if not exist
release_dir="${SCRIPT_PATH}/cmake-build-release"
debug_dir="${SCRIPT_PATH}/cmake-build-debug"



if [ $1 = "release" ]; then
  if [ ! -d ${release_dir} ];then
  mkdir -p ${release_dir}
fi
  cd ${release_dir}
  cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. && cmake --build . -- -j $CPU_CORES
elif [ $1 = "profile" ]; then
  cd ${release_dir}
  cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS=-pg -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. && cmake --build . -- -j $CPU_CORES
else
  mkdir -p ${debug_dir}
  cd ${debug_dir}
  cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. && cmake --build . -- -j $CPU_CORES
fi
