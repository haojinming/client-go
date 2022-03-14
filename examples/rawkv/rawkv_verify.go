// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
)

var (
	pdMain        string
	pdRecovery    string
	threads       int64
	keysPerThread int64
	valueBase     int
	sleep         int
	logLevel      string
	cmdPut        bool
	cmdVerify     bool
	cmdFDTest     bool
	paddingLen    int
	padding       string
)

const VALUE_BASE_MAX = 10000
const DEFAULT_PADDING_LEN = 64
const KEY_RANGE = 1e8

func init() {
	flag.BoolVar(&cmdPut, "put", false, "PUT workload")
	flag.BoolVar(&cmdVerify, "verify", false, "VERIFY result")
	flag.BoolVar(&cmdFDTest, "fdtest", false, "do fd test")
	flag.StringVar(&pdMain, "main", "http://127.0.0.1:2379", "main cluster pd addr, default: http://127.0.0.1:2379")
	flag.StringVar(&pdRecovery, "recovery", "http://127.0.0.1:2379", "secondary cluster pd addr, default: http://127.0.0.1:2379")
	flag.Int64Var(&threads, "threads", 10, "# of threads")
	flag.Int64Var(&keysPerThread, "keys", 10, "# of keys per thread")
	flag.IntVar(&valueBase, "value", 0, "value to put / verify. Unset or 0 to get a random value")
	flag.IntVar(&sleep, "sleep", 1, "sleep seconds between PUT & Verify")
	flag.StringVar(&logLevel, "log_level", "info", "log level [debug/info/error]")
	flag.IntVar(&paddingLen, "pad", DEFAULT_PADDING_LEN, "value padding length")
	flag.Parse()

	conf := &log.Config{Level: logLevel, File: log.FileLogConfig{
		Filename: "rawkv_verify.log",
	}}
	logger, props, _ := log.InitLogger(conf)
	log.ReplaceGlobals(logger, props)

	if valueBase == 0 {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		valueBase = r.Intn(VALUE_BASE_MAX) + 1
	}

	padding = strings.Repeat("0", paddingLen)
}

func doPut() {
	var putCnt uint64 = 0
	var wg sync.WaitGroup
	for i := int64(0); i < threads; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())

			cli, err := rawkv.NewClient(ctx, []string{pdMain}, config.DefaultConfig().Security)
			if err != nil {
				panic(err)
			}
			defer cli.Close()

			log.Info("PUT worker start", zap.Uint64("clusterID", cli.ClusterID()), zap.Int64("thread", i))

			step := int64(KEY_RANGE / keysPerThread / threads)
			if step <= 0 {
				step = 1
			}
			start := i * keysPerThread * step
			end := (i + 1) * keysPerThread * step
			for k := start; k < end; k += step {
				key := fmt.Sprintf("vk%08d", k)
				val_1 := fmt.Sprintf("%v_%v", valueBase-1, padding)
				val := fmt.Sprintf("%v_%v", valueBase, padding)
				log.Debug("PUT", zap.String("key", key), zap.String("val-1", val_1), zap.String("val", val))

				err = cli.Put(ctx, []byte(key), []byte(val))
				putCnt++
				if err != nil {
					panic(err)
				}
			}

			cancel()
		}()
	}
	wg.Wait()
	fmt.Fprintf(os.Stderr, "PUT end, total cnt %v\n", putCnt)
}

func doVerify() {
	var wg sync.WaitGroup
	var errCount int = 0
	var getCnt uint64 = 0
	for i := int64(0); i < threads; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())

			cli, err := rawkv.NewClient(ctx, []string{pdMain}, config.DefaultConfig().Security)
			if err != nil {
				panic(err)
			}
			defer cli.Close()

			log.Info("VERIFY worker start", zap.Uint64("clusterID", cli.ClusterID()), zap.Int64("thread", i))

			step := int64(KEY_RANGE / keysPerThread / threads)
			if step <= 0 {
				step = 1
			}
			start := i * keysPerThread * step
			end := (i + 1) * keysPerThread * step
			for k := start; k < end; k += step {
				key := fmt.Sprintf("vk%08d", k)
				expected := fmt.Sprintf("%v", valueBase)
				getCnt++
				val, err := cli.Get(ctx, []byte(key))
				if err != nil {
					errCount = errCount + 1
					log.Error("VERIFY ERROR,", zap.String("key", key), zap.String("got", string(val)), zap.Int("paddingLen", paddingLen))
					fmt.Fprintf(os.Stderr, "VERIFY ERROR,: key: %v, got: %v, paddingLen: %v\n", key, string(val), paddingLen)
					continue
				}
				if len(val) < paddingLen+1 {
					log.Error("VERIFY ERROR, len(val) < paddingLen+1", zap.String("key", key), zap.String("got", string(val)), zap.Int("paddingLen", paddingLen))
					fmt.Fprintf(os.Stderr, "VERIFY ERROR, len(val) < paddingLen+1: key: %v, got: %v, paddingLen: %v\n", key, string(val), paddingLen)
				} else {
					val = val[0 : len(val)-paddingLen-1]
					log.Debug("VERIFY", zap.String("key", key), zap.String("val-expected", expected), zap.String("got", string(val)))

					if string(val) != expected {
						log.Error("VERIFY ERROR", zap.String("key", key), zap.String("val-expected", expected), zap.String("got", string(val)))
						fmt.Fprintf(os.Stderr, "VERIFY ERROR: key: %v, value-expect: %v, got: %v\n", key, expected, string(val))
					}
				}
			}
			log.Debug("VERIFY, finish, got ", zap.Int("err", errCount))
			fmt.Fprintf(os.Stderr, "VERIFY, finish, cur total %v, got %v err.\n", getCnt, errCount)

			cancel()
		}()
	}
	wg.Wait()
}

func generateTestKeyValue(keyPrefix []byte, valPrefix []byte, seriNo int64) (string, string) {
	key := fmt.Sprintf("%s_%d", keyPrefix, seriNo)
	value := fmt.Sprintf("%s_%s", valPrefix, key)
	return key, value
}

func generateTestBatchKeyValue(keyPrefix []byte, valPrefix []byte, startSeriNo int64, cnt int64) ([][]byte, [][]byte) {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i := startSeriNo; i < startSeriNo+cnt; i++ {
		key, val := generateTestKeyValue(keyPrefix, valPrefix, i)
		rawKeys = append(rawKeys, []byte(key))
		rawValues = append(rawValues, []byte(val))
	}
	return rawKeys, rawValues
}

func putBatchKV(ctx context.Context, cli *rawkv.Client, keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return errors.New("different key/val len")
	}

	for i := 0; i < len(keys); i++ {
		err := cli.Put(ctx, keys[i], vals[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func testRawPutGet(keyCnt int64) {

	ctx, cancel := context.WithCancel(context.Background())
	cli, err := rawkv.NewClient(ctx, []string{pdMain}, config.DefaultConfig().Security)
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	defer cancel()
	for i := int64(0); i < keyCnt; i++ {
		key, val := generateTestKeyValue([]byte("testRawPutGet"), []byte("value"), i)
		err := cli.Put(ctx, []byte(key), []byte(val))
		if err != nil {
			fmt.Fprintf(os.Stderr, "testRawPutGet put fail, cur index: %d.\n", i)
			return
		}
	}

	for i := int64(0); i < keyCnt; i++ {
		key, val := generateTestKeyValue([]byte("testRawPutGet"), []byte("value"), i)
		retVal, err := cli.Get(ctx, []byte(key))
		if err != nil || string(retVal) != val {
			fmt.Fprintf(os.Stderr, "testRawPutGet get fail, return %v, expect val:%v, return val:%v.\n", err, val, retVal)
			return
		}
	}

	fmt.Fprintf(os.Stdout, "testRawPutGet pass.\n")
}

func testRawBatchPutBatchGet() {
	ctx, cancel := context.WithCancel(context.Background())
	cli, err := rawkv.NewClient(ctx, []string{pdMain}, config.DefaultConfig().Security)
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	defer cancel()

	start := int64(100)
	batchCnt := int64(1000)

	rawKeys, rawVals := generateTestBatchKeyValue([]byte("testRawBatchPutBatchGet"), []byte("value"), start, batchCnt)
	err = cli.BatchPut(ctx, rawKeys, rawVals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testRawBatchPutBatchGet put fail.\n")
		return
	}

	for i := int64(0); i < batchCnt; i++ {
		retVal, err := cli.Get(ctx, rawKeys[i])
		if err != nil || !bytes.Equal(retVal, rawVals[i]) {
			fmt.Fprintf(os.Stderr, "testRawBatchPutBatchGet get fail, return %v, expect val:%v, return val:%v.\n", err, rawVals[i], retVal)
			return
		}
	}

	retVals, err := cli.BatchGet(ctx, rawKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testRawBatchPutBatchGet get fail.\n")
		return
	}

	for i := int64(0); i < batchCnt; i++ {
		if !bytes.Equal(retVals[i], rawVals[i]) {
			fmt.Fprintf(os.Stderr, "testRawBatchPutBatchGet get val fail, cur index: %d, expect:%s, ret:%s.\n",
				i, rawVals[i], retVals[i])
			return
		}
	}
	fmt.Fprintf(os.Stdout, "testRawBatchPutBatchGet pass.\n")
}

func tesRawtCompareAndSwap() {
	fmt.Fprintf(os.Stdout, "testRawBatchPutBatchGet not implemented.\n")
}

func testRawSameKeyWrite() {
	ctx, cancel := context.WithCancel(context.Background())
	cli, err := rawkv.NewClient(ctx, []string{pdMain}, config.DefaultConfig().Security)
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	defer cancel()

	// preload some keys
	start := int64(100)
	batchCnt := int64(1000)
	rawKeys, rawVals := generateTestBatchKeyValue([]byte("testRawSameKeyWrite"), []byte("value"), start, batchCnt)
	err = cli.BatchPut(ctx, rawKeys, rawVals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testRawSameKeyWrite preload fail.\n")
		return
	}
	// pick one key to write multi versions.
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	index := r.Int63n(batchCnt)
	var midKey = rawKeys[index]
	var midVal = rawVals[index]
	var putVal = midVal
	putCnt := r.Intn(200)
	for i := 0; i < putCnt; i++ {
		putVal = []byte(string(i) + string(midVal))
		err := cli.Put(ctx, midKey, midVal)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testRawSameKeyWrite put fail, cur index: %d.\n", i)
			return
		}
	}
	retVal, err := cli.Get(ctx, midKey)
	if err != nil || !bytes.Equal(putVal, retVal) {
		fmt.Fprintf(os.Stderr, "testRawSameKeyWrite get val fail, expect:%s, ret:%s.\n", midVal, retVal)
		return
	}

	fmt.Fprintf(os.Stdout, "testRawSameKeyWrite pass.\n")
}

func testRawKeyTTL() {

}

func testRawKeyMvcc() {

}

// include scan / reverse scan / batch scan
func testRawScan() {

}

// include delete / batch delete / delete range
func testRawDelete() {

}

func main() {
	fmt.Printf("Value Base: %v\n", valueBase)

	if cmdPut {
		doPut()
	} else if cmdVerify {
		doVerify()
	} else if cmdFDTest {
		testRawPutGet(100)
		testRawBatchPutBatchGet()
	} else {
		fmt.Printf("Do PUT now.\n")
		doPut()

		fmt.Printf("PUT finished. Waiting for %v seconds... ", sleep)
		time.Sleep(time.Duration(sleep) * time.Second)
		fmt.Printf("Do VERIFY now.\n")

		doVerify()
		fmt.Printf("DONE.\n")
	}
}
