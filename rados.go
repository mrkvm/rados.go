// Package rados provides Go bindings for the CEPH RADOS client library (librados)
// We attempt to adhere to the style of the Go OS package as much as possible
// (for example, our Object type implements the FileStat and ReaderAt/WriterAt
// interfaces).
package rados

/*
#cgo LDFLAGS: -lrados
#include "stdlib.h"
#include "rados/librados.h"
*/
import "C"

import (
    "fmt"
    "unsafe"
)

// Rados provides a handle for interacting with a RADOS cluster.
type Rados struct {
    rados      C.rados_t
    size       uint64
    used       uint64
    avail      uint64
    numObjects uint64
}

// New returns a RADOS cluster handle that is used to create IO
// Contexts and perform other RADOS actions. If configFile is
// non-empty, RADOS will look for its configuration there, otherwise
// the default paths will be searched (e.g., /etc/ceph/ceph.conf).
//
// TODO: allow caller to specify Ceph user.
func New(configFile string) (*Rados, error) {
    r := &Rados{}
    var cerr C.int

    if cerr = C.rados_create(&r.rados, nil); cerr < 0 {
        return nil, fmt.Errorf("RADOS create: %s", strerror(cerr))
    }

    if configFile == "" {
        cerr = C.rados_conf_read_file(r.rados, nil)
    } else {
        cconfigFile := C.CString(configFile)
        defer C.free(unsafe.Pointer(cconfigFile))

        cerr = C.rados_conf_read_file(r.rados, cconfigFile)
    }

    if cerr < 0 {
        return nil, fmt.Errorf("RADOS config: %s", strerror(cerr))
    }

    if cerr = C.rados_connect(r.rados); cerr < 0 {
        return nil, fmt.Errorf("RADOS connect: %s", strerror(cerr))
    }

    // Fill in cluster statistics
    if err := r.Stat(); err != nil {
        r.Release()
        return nil, err
    }

    return r, nil
}

// NewDefault returns a RADOS cluster handle based on the default config file.
// See New() for more information.
func NewDefault() (r *Rados, err error) {
    r, err = New("")
    return r, err
}

// Stat retrieves the current cluster statistics and stores them in
// the Rados structure.
func (r *Rados) Stat() error {
    var cstat C.struct_rados_cluster_stat_t

    if cerr := C.rados_cluster_stat(r.rados, &cstat); cerr < 0 {
        return fmt.Errorf("RADOS cluster stat: %s", strerror(cerr))
    }

    r.size = uint64(cstat.kb)
    r.used = uint64(cstat.kb_used)
    r.avail = uint64(cstat.kb_avail)
    r.numObjects = uint64(cstat.num_objects)

    return nil
}

// Size returns the total size of the cluster in kilobytes.
func (r *Rados) Size() uint64 {
    return r.size
}

// Used returns the number of used kilobytes in the cluster.
func (r *Rados) Used() uint64 {
    return r.used
}

// Avail returns the number of available kilobytes in the cluster.
func (r *Rados) Avail() uint64 {
    return r.avail
}

// NumObjects returns the number of objects in the cluster.
func (r *Rados) NumObjects() uint64 {
    return r.numObjects
}

// Release handle and disconnect from RADOS cluster.
//
// TODO: track all open ioctx, ensure all async operations have
// completed before calling rados_shutdown, because it doesn't do that
// itself.
func (r *Rados) Release() error {
    C.rados_shutdown(r.rados)

    return nil
}

// PoolCreate creates the named pool in the given RADOS cluster.
// PoolCreate uses the default admin user and crush rule.
//
// TODO: Add ability to create pools with specific admin users/crush rules.
func (r *Rados) PoolCreate(poolName string) error {
    cname := C.CString(poolName)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_pool_create(r.rados, cname); cerr < 0 {
        return fmt.Errorf("RADOS pool create %s: %s", poolName, strerror(cerr))
    }

    return nil
}

// PoolDelete deletes the named pool in the given RADOS cluster.
func (r *Rados) PoolDelete(poolName string) error {
    cname := C.CString(poolName)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_pool_delete(r.rados, cname); cerr < 0 {
        return fmt.Errorf("RADOS pool delete %s: %s", poolName, strerror(cerr))
    }

    return nil
}

// byteSliceToBuffer is a utility function to convert the given byte slice
// to a C character pointer. It returns the pointer and the size of
// the data (as a C size_t).
func byteSliceToBuffer(data []byte) (*C.char, C.size_t) {
    if len(data) > 0 {
        return (*C.char)(unsafe.Pointer(&data[0])), C.size_t(len(data))
    } else {
        return (*C.char)(unsafe.Pointer(&data)), C.size_t(0)
    }
}

// strerror is a utility wrapper around the libc strerror() function. It returns
// a Go string containing the text of the error.
func strerror(cerr C.int) string {
    return C.GoString(C.strerror(-cerr))
}
