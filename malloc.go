package godis

import (
	"syscall"
)

func Malloc(size int) Bytes {
	return Bytes{
		ptr: malloc(size),
		len: size,
		cap: size,
	}
}

func malloc(size int) uintptr {
	fd := -1
	p, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP,
		0, uintptr(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE,
		uintptr(fd), // fd
		0,           // offset
	)
	if errno != 0 {
		panic(errno)
	}
	return p
}

func Free(bytes Bytes) {
	free(bytes.ptr, bytes.cap)
}

// todo pool
func free(ptr uintptr, size int) {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, ptr, uintptr(size), 0)
	if errno != 0 {
		panic(errno)
	}
}
