# duplicate-file-spot-checker

A Go tool that scans your filesystem for duplicate files by comparing sample chunks rather than full file contents. Skips virtual/system filesystems (tmpfs, sysfs, etc.) and gives you stats on what it finds.

## Usage

```
go build && ./duplicate-file-spot-checker [-j NUM_CORES] [path]
```

- `-j` - number of parallel cores to use
