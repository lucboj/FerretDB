#!/bin/bash

mkdir tmpDir \
&& curl https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz --output tmpDir/hanaclient.tar.gz \
&& tar -xzvf tmpDir/hanaclient.tar.gz -C tmpDir \
&& driver/client/./hdbinst --batch --ignore=check_diskspace \
&& mv /usr/sap/hdbclient/golang/src/SAP /usr/local/go/src/ \
    && ( \
        cd /usr/sap/hdbclient/golang/src/ \
        && go install SAP/go-hdb/driver ) \
    && rm -rf tmpDir


export PATH=$PATH:/usr/sap/hdbclient
export CGO_LDFLAGS=/usr/sap/hdbclient/libdbcapiHDB.so
export GO111MODULE=auto
export LD_LIBRARY_PATH=/usr/sap/hdbclient/