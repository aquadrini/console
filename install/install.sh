#!/bin/bash

function makeDirs() {
    for i in {1..3}; do
        mkdir -p /tmp/zookeeper$i/data
        mkdir -p /tmp/zookeeper$i/log
        echo "$i" > /tmp/zookeeper$i/data/myid
    done
}

function unpack() {
    actual=$(pwd)
    cp zookeeper-3.4.6.tar.gz $1
    cd $1
    tar xvzf zookeeper-3.4.6.tar.gz
    cd zookeeper-3.4.6
    rm -rf conf
    cp -r $actual/conf .
    cp $actual/startCluster.sh bin
}

function checkArgs() {
    if [ "$1" != 1 ]; then
        echo "Must specify destination directory"
        return 1
    else
        return 0
    fi
}

checkArgs $#
if [ "$?" == 0 ]; then
    unpack $1
    makeDirs
fi