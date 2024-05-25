#!/bin/bash

# basic map-reduce test

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

echo "Building MR apps..."
echo $(pwd)

# make sure software is freshly built.
(cd ../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

echo '***' Starting reduce parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../../data/pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../mrapps/rtiming.so &
timeout -k 2s 180s ../mrworker ../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo "$NT"
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo "$NT"
  echo '---' reduce parallelism test: PASS
fi