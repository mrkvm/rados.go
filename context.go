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

// Context represents a RADOS IO context for the pool Pool.
type Context struct {
    Pool string
    ctx  C.rados_ioctx_t
}

// NewContext creates a new RADOS IO context for a given pool, which used to
// do IO operations. The pool must exist (see Rados.PoolCreate()).
func (r *Rados) NewContext(pool string) (*Context, error) {
    if r.rados == nil {
        return nil, fmt.Errorf("RADOS not connected")
    }

    cpool := C.CString(pool)
    defer C.free(unsafe.Pointer(cpool))

    c := &Context{Pool: pool}

    if cerr := C.rados_ioctx_create(r.rados, cpool, &c.ctx); cerr < 0 {
        return nil, fmt.Errorf("RADOS new ioctx for pool %s: %s",
            strerror(cerr))
    }

    return c, nil
}

// Release this RADOS IO context.
//
// TODO: track all uncompleted async operations before calling
// rados_ioctx_destroy(), because it doesn't do that itself.
func (c *Context) Release() error {
    C.rados_ioctx_destroy(c.ctx)

    return nil
}

// PoolInfo provides usage information about a pool
type PoolInfo struct {
    BytesUsed                uint64
    KBytesUsed               uint64
    NObjects                 uint64
    NObjectClones            uint64
    NObjectCopies            uint64
    NObjectsMissingOnPrimary uint64
    NObjectsUnfound          uint64
    NObjectsDegraded         uint64
    BytesRead                uint64
    BytesWritten             uint64
    KBytesRead               uint64
    KBytesWritten            uint64
}

// PoolStat retrieves the current usage for pool referenced by the
// given context and returns them in the PoolInfo structure.
func (c *Context) PoolStat() (*PoolInfo, error) {
    var pstat C.struct_rados_pool_stat_t

    if cerr := C.rados_ioctx_pool_stat(c.ctx, &pstat); cerr < 0 {
        return nil, fmt.Errorf("RADOS pool stat: %s", strerror(cerr))
    }

    info := &PoolInfo{
        BytesUsed:                uint64(pstat.num_bytes),
        KBytesUsed:               uint64(pstat.num_kb),
        NObjects:                 uint64(pstat.num_objects),
        NObjectClones:            uint64(pstat.num_object_clones),
        NObjectCopies:            uint64(pstat.num_object_copies),
        NObjectsMissingOnPrimary: uint64(pstat.num_objects_missing_on_primary),
        NObjectsUnfound:          uint64(pstat.num_objects_unfound),
        NObjectsDegraded:         uint64(pstat.num_objects_degraded),
        BytesRead:                uint64(pstat.num_rd),
        BytesWritten:             uint64(pstat.num_wr),
        KBytesRead:               uint64(pstat.num_rd_kb),
        KBytesWritten:            uint64(pstat.num_wr_kb),
    }

    return info, nil
}
