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

type Context struct {
    Pool string
    ctx  C.rados_ioctx_t
}

/* NewContext creates a new RADOS IO context for a given pool, used to
 * do IO operations. The pool must exist (see Rados.CreatePool()).
 */
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

/* Release this RADOS IO context.
 *
 * TODO: track all uncompleted async operations before calling
 * rados_ioctx_create, because it doesn't do that itself.
 */
func (c *Context) Release() error {
    C.rados_ioctx_destroy(c.ctx)

    return nil
}
