package main

import (
	"fmt"
	"golang.org/x/sys/unix"
)

// getFsType returns the filesystem type for the given file path.
// It uses statfs syscall to get the filesystem type magic number.
func getFsType(path string) (string, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return "", fmt.Errorf("statfs failed for %s: %w", path, err)
	}

	return fsTypeToString(stat.Type), nil
}

// fsTypeToString converts a filesystem type magic number to a human-readable string.
// Magic numbers are defined in the Linux kernel (include/uapi/linux/magic.h).
func fsTypeToString(fsType int64) string {
	switch fsType {
	case 0x01021994:
		return "tmpfs"
	case 0x62656572:
		return "sysfs"
	case 0xEF53:
		return "ext2/ext3/ext4"
	case 0x6969:
		return "nfs"
	case 0x58465342:
		return "xfs"
	case 0x9123683E:
		return "btrfs"
	case 0x73717368:
		return "squashfs"
	case 0x137D:
		return "ext"
	case 0x4244:
		return "hfs"
	case 0x4d44:
		return "msdos/fat"
	case 0x52654973:
		return "reiserfs"
	case 0x6165676C:
		return "smb"
	case 0xFF534D42:
		return "cifs"
	case 0x5346544e:
		return "ntfs"
	case 0x9fa0:
		return "proc"
	case 0x27e0eb:
		return "cgroup"
	case 0x63677270:
		return "cgroup2"
	case 0x42465331:
		return "befs"
	case 0x1badface:
		return "bfs"
	case 0x42494e4d:
		return "binfmt_misc"
	case 0xcafe4a11:
		return "bpf_fs"
	case 0x9fa1:
		return "openprom"
	case 0x50495045:
		return "pipefs"
	case 0x002f:
		return "qnx4"
	case 0x68191122:
		return "qnx6"
	case 0x858458f6:
		return "ramfs"
	case 0x52445435, 0x7275:
		return "romfs"
	case 0x67596969:
		return "rpc_pipefs"
	case 0x73636673:
		return "securityfs"
	case 0xf97cff8c:
		return "selinux"
	case 0x43415d53:
		return "smack"
	case 0x534F434B:
		return "sockfs"
	case 0x74726163:
		return "tracefs"
	case 0x01021997:
		return "v9fs"
	case 0x565a4653:
		return "vxfs"
	case 0xabba1974:
		return "xenfs"
	case 0x012ff7b4:
		return "xia"
	case 0x012ff7b5:
		return "xiafs"
	case 0x012ff7b6:
		return "overlay"
	case 0x794c7630:
		return "overlayfs"
	case 0xaad7aaea:
		return "panfs"
	case 0x64626720:
		return "debugfs"
	case 0x47504653:
		return "gpfs"
	case 0x6a656a62:
		return "jffs2"
	case 0x2011bab0:
		return "exfat"
	case 0x19830326:
		return "fhgfs"
	case 0x65735546:
		return "fuse"
	case 0x65735543:
		return "fusectl"
	case 0xbad1dea:
		return "futexfs"
	case 0x4006:
		return "fat"
	case 0x4d5a:
		return "minix"
	case 0x2468, 0x2478, 0x138F:
		return "minix2"
	case 0x564c:
		return "ncp"
	case 0x517b:
		return "smb"
	case 0x6e736673:
		return "nsfs"
	case 0x5346414F:
		return "openafs/afs"
	case 0xadf5:
		return "adfs"
	case 0xadff:
		return "affs"
	case 0x0187:
		return "autofs"
	case 0x62646576:
		return "bdevfs"
	default:
		return fmt.Sprintf("unknown(0x%x)", fsType)
	}
}
