// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
//	"crypto/hmac"
	"crypto/md5"
//	"crypto/sha1"
//	"crypto/tls"
//	"encoding/base64"
	"flag"
	"fmt"
	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
//	"github.com/pivotal-golang/bytefmt"
//	"io"
//	"io/ioutil"
	"log"
	"math/rand"
//	"net"
	"net/http"
	"os"
//	"sort"
//	"strconv"
//	"strings"
//	"sync"
	"sync/atomic"
	"time"
)

// Global variables
var bucket string
var duration_secs, threads, loops int
var object_size uint64
var object_data []byte
var running_threads, upload_count, download_count, upload_slowdown_count, download_slowdown_count int32
var endtime, upload_finish, download_finish, delete_finish time.Time

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

func _getFile(sess *session.Session) {
	atomic.AddInt32(&download_count, 1)
	objnum := rand.Int31n(download_count) + 1
	filename := fmt.Sprintf("Object-%d", objnum)
	downloader := s3manager.NewDownloader(sess)
	buff := &aws.WriteAtBuffer{}

	_, err := downloader.Download(buff,
	&s3.GetObjectInput{
	    Bucket: &bucket,
	    Key:    &filename,
	})

	if err != nil {
		atomic.AddInt32(&download_slowdown_count, 1)
	}
}

func runGetFile(thread_num int) {
//	sess := session.Must(session.NewSessionWithOptions(session.Options{
//	SharedConfigState: session.SharedConfigEnable,
//	}))

	for time.Now().Before(endtime) {
		atomic.AddInt32(&download_count, 1)
		objnum := rand.Int31n(download_count) + 1
		filename := fmt.Sprintf("%d, Object-%d", download_count, objnum)
		fmt.Println("getFile, %s", filename)
		time.Sleep(time.Millisecond * 20)
//		_getFile(sess)
		//fmt.Println("run getFile")
	}

	// Remember last done time
	download_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func _putFile(sess *session.Session) {

    objnum := atomic.AddInt32(&upload_count, 1)
    filename := fmt.Sprintf("Object-%d", objnum)
    file:= bytes.NewReader(object_data)

    uploader := s3manager.NewUploader(sess)
    _, err := uploader.Upload(&s3manager.UploadInput{
        Bucket: &bucket,
        Key:    &filename,
        Body:   file,
    })
    if err != nil {
	atomic.AddInt32(&upload_slowdown_count, 1)
    }

}

func runPutFile(thread_num int) {
//	sess := session.Must(session.NewSessionWithOptions(session.Options{
//	SharedConfigState: session.SharedConfigEnable,
//	}))

	for time.Now().Before(endtime) {
		atomic.AddInt32(&upload_count, 1)
		time.Sleep(time.Millisecond * 20)
		//_putFile(sess)
		//fmt.Println("run putFile")
	}

	// Remember last done time
	upload_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func main() {
	// Hello

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&bucket, "b", "zilliz-hz01", "Bucket for testing")
	myflag.IntVar(&duration_secs, "d", 1, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	logit(fmt.Sprintf("Parameters: bucket=%s, duration=%d, threads=%d, loops=%d, size=%s",
		bucket, duration_secs, threads, loops, sizeArg))

	// Initialize data for the bucket
	object_data = make([]byte, object_size)
	rand.Read(object_data)
	hasher := md5.New()
	hasher.Write(object_data)
//	runPutFile()
//	runGetFile()

	// reset counters
	upload_count = 0
	upload_slowdown_count = 0
	download_count = 0
	download_slowdown_count = 0

	// Run the upload case
	running_threads = int32(threads)

	starttime := time.Now()
	endtime = starttime.Add(time.Second * time.Duration(duration_secs))

	for n := 1; n <= threads; n++ {
		go runPutFile(n)
	}

	// Wait for it to finish
	for atomic.LoadInt32(&running_threads) > 0 {
		time.Sleep(time.Millisecond)
	}
	upload_time := upload_finish.Sub(starttime).Seconds()

	loop := 1
	bps := float64(uint64(upload_count)*object_size) / upload_time
	logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
		loop, upload_time, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_count)/upload_time, upload_slowdown_count))

	// Run the download case
	running_threads = int32(threads)
	starttime = time.Now()
	endtime = starttime.Add(time.Second * time.Duration(duration_secs))
	for n := 1; n <= threads; n++ {
		go runGetFile(n)
	}

	// Wait for it to finish
	for atomic.LoadInt32(&running_threads) > 0 {
		time.Sleep(time.Millisecond)
	}
	download_time := download_finish.Sub(starttime).Seconds()

	bps = float64(uint64(download_count)*object_size) / download_time
	logit(fmt.Sprintf("Loop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
		loop, download_time, download_count, bytefmt.ByteSize(uint64(bps)), float64(download_count)/download_time, download_slowdown_count))

	// All done
}
