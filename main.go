package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	//_ "os/signal"
	"strings"
	//_ "syscall"
)

var blacklistFilesystems = []string{"tmpfs", "sysfs", "efivarfs", "devfs", "tracefs"}

// isBlacklistedFilesystem checks if the filesystem type is in the blacklist.
// Cache trades memory for speed: faster for many files on few filesystem types,
// but likely overkill given the small blacklist size (3 items) and cheap string operations.
var isBlacklistCache = make(map[string]bool)

func isBlacklistedFilesystem(fstype string) bool {
	is, found := isBlacklistCache[fstype]
	if found {
		return is
	}
	for _, blacklisted := range blacklistFilesystems {
		if strings.Contains(fstype, blacklisted) {
			isBlacklistCache[fstype] = true
			return true
		}
	}
	isBlacklistCache[fstype] = false
	return false
}

func outPutFileGroup(fg []string) {
	for _, f := range fg {
		fmt.Printf("%s\n", f)
	}
	fmt.Printf("\n")
}

func main() {
	flag.IntVar(&concurrentHashes, "j", 8, "number of concurrent hash operations")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	analyser := newDuplicateFileAnalyser()
	stats := newScanStats()

	analyser.consumeErrors(func(e error) {
		fmt.Fprintf(os.Stderr, "error analysing: %v\n", e)
		stats.incrementStat("reading files")
	},
	)

	for scanner.Scan() {
		filename := strings.TrimSpace(scanner.Text())
		if filename == "" {
			fmt.Fprintf(os.Stderr, "skipping blank line with scanner: %v\n", scanner)
			stats.incrementStat("blank line")
			continue
		}

		stat, err := os.Lstat(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting file info for %s: %v\n", filename, err)
			stats.incrementStat("stat error")
			continue
		}

		mode := stat.Mode()
		if mode.IsDir() {
			stats.incrementStat("directory")
			continue
		}

		if mode&os.ModeSymlink != 0 {
			stats.incrementStat("symlink")
			continue
		} else if mode&os.ModeType != 0 {
			fmt.Fprintf(os.Stderr, "skipping special file: %s (mode: %s)\n", filename, mode)
			stats.incrementStat("special file")
			continue
		}

		fstype, err := getFsType(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: couldn't determine fs type for %s\n", filename)
		}
		if isBlacklistedFilesystem(fstype) {
			fmt.Fprintf(os.Stderr, "skipping file: %s, its on a %s filesystem\n", filename, fstype)
			stats.incrementStat("blacklisted filesystem: " + fstype)
			continue
		}

		if stat.Size() == 0 {
			stats.incrementStat("zero length files")
			continue
		}

		analyser.add(filename, stat.Size())
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
	}
	analyser.finish()

	for _, group := range analyser.groups() {
		if len(group) > 1 {
			outPutFileGroup(group)
		}
	}

	fmt.Fprint(os.Stderr, stats.dump())
}
