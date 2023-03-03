#!/bin/bash

current_dir="$(dirname "$(pwd)")"

echo $current_dir

mkdir tmpDir \
&& curl https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz --output tmpDir/hanaclient.tar.gz \
&& tar -xzvf tmpDir/hanaclient.tar.gz -C tmpDir \
&& driver/client/./hdbinst --batch --ignore=check_diskspace \
&& mv /home/runner/sap/hdbclient/golang/src/SAP /opt/hostedtoolcache/go/1.20.1/x64/src/ \
    && ( \
        cd home/runner/sap/hdbclient/golang/src/ \
        && go install SAP/go-hdb/driver ) \
    && rm -rf tmpDir


pwd

export PATH=$PATH:/home/runner/sap/hdbclient
export CGO_LDFLAGS=/home/runner/sap/hdbclient/libdbcapiHDB.so
export GO111MODULE=auto
export LD_LIBRARY_PATH=/home/runner/hdbclient/