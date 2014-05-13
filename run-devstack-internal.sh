#!/bin/sh

scriptdir=$( dirname "$0" )
. "$scriptdir/devstack-internalrc"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$scriptdir/../swift-client/Debug:$scriptdir/../keystone-client/Debug
"$scriptdir/Debug/test-swift-client" --verbose
