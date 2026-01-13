package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"os"
	//_ "os/signal"
	"sync"
	//_ "syscall"
)

const blocksize = 8192

var concurrentHashes = 8
var skipBlocks = 1000
var sizeThreshold int64 = 8 * blocksize

func getHash(path string) (uint64, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	hasher := fnv.New64()
	data := make([]byte, blocksize)
	for {
		bytesRead, readErr := fh.Read(data)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
		hasher.Write(data[:bytesRead])
	}
	return hasher.Sum64(), nil
}

func getSparseHash(path string, size int64) (uint64, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	hasher := fnv.New64()
	data := make([]byte, blocksize)
	for offs := 0; int64(offs) < size; offs += blocksize * skipBlocks {
		bytesRead, readErr := fh.ReadAt(data, int64(offs))
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
		hasher.Write(data[:bytesRead])
	}
	return hasher.Sum64(), nil
}

type fileInfo struct {
	name string
	size int64
}

type duplicateFileAnalyser struct {
	bySizeByHash   map[int64]map[uint64][]string
	bySizeMutex    sync.RWMutex
	byHashMutexes  map[int64]*sync.Mutex
	processWG      sync.WaitGroup
	filenameChan   chan fileInfo
	errors         chan error
	errorWaitgroup sync.WaitGroup
}

func newDuplicateFileAnalyser() *duplicateFileAnalyser {
	dfa := &duplicateFileAnalyser{
		bySizeByHash:   make(map[int64]map[uint64][]string),
		bySizeMutex:    sync.RWMutex{},
		byHashMutexes:  make(map[int64]*sync.Mutex),
		processWG:      sync.WaitGroup{},
		filenameChan:   make(chan fileInfo),
		errors:         make(chan error),
		errorWaitgroup: sync.WaitGroup{},
	}
	for i := 0; i < concurrentHashes; i++ {
		go dfa.process()
	}
	return dfa
}

func (d *duplicateFileAnalyser) processFile(fi fileInfo) {
	d.processWG.Add(1)
	defer d.processWG.Done()

	d.bySizeMutex.RLock()
	_, has := d.bySizeByHash[fi.size]
	d.bySizeMutex.RUnlock()

	if !has {
		d.bySizeMutex.Lock()
		_, has2 := d.bySizeByHash[fi.size]
		if !has2 {
			d.bySizeByHash[fi.size] = make(map[uint64][]string)
			d.byHashMutexes[fi.size] = &sync.Mutex{}
		}
		d.bySizeMutex.Unlock()
	}
	d.bySizeMutex.RLock()
	byHash := d.bySizeByHash[fi.size]
	byHashMutex := d.byHashMutexes[fi.size]
	d.bySizeMutex.RUnlock()

	var hashval uint64
	var err error
	if fi.size < sizeThreshold {
		hashval, err = getHash(fi.name)
	} else {
		//d.errors <- fmt.Errorf("%s is bigger than %v bytes, using sparse hash", fi.name, sizeThreshold)
		hashval, err = getSparseHash(fi.name, fi.size)
	}
	if err != nil {
		d.errors <- fmt.Errorf("error hashing %s: %v", fi.name, err)
		return
	}

	byHashMutex.Lock()
	_, has = byHash[hashval]
	if !has {
		byHash[hashval] = []string{}
	}
	byHash[hashval] = append(byHash[hashval], fi.name)
	byHashMutex.Unlock()
}

func (d *duplicateFileAnalyser) process() {
	for fi := range d.filenameChan {
		d.processFile(fi)
	}
}

func (d *duplicateFileAnalyser) add(filename string, size int64) error {
	d.filenameChan <- fileInfo{filename, size}

	return nil
}

func (d *duplicateFileAnalyser) finish() {
	d.processWG.Wait()
	close(d.filenameChan)
	d.errorWaitgroup.Wait()
	close(d.errors)
}

func (d *duplicateFileAnalyser) consumeErrors(f func(error)) {
	go func() {
		for e := range d.errors {
			d.errorWaitgroup.Add(1)
			f(e)
			d.errorWaitgroup.Done()
		}
	}()
}

func (d *duplicateFileAnalyser) groups() [][]string {
	groups := [][]string{}
	for byHash := range maps.Values(d.bySizeByHash) {
		for fileGroup := range maps.Values(byHash) {
			groups = append(groups, fileGroup)
		}
	}
	return groups
}

func (d *duplicateFileAnalyser) dump() string {

	numSizeBuckets := len(d.bySizeByHash)
	numFiles := 0
	for v := range maps.Values(d.bySizeByHash) {
		for vv := range maps.Values(v) {
			numFiles += len(vv)
		}
	}

	return fmt.Sprintf("%d files analysed, %d size buckets", numSizeBuckets, numFiles)
}
