package rados

/*
#cgo LDFLAGS: -lrados
#include "stdlib.h"
#include "stdint.h"
#include "rados/librados.h"
*/
import "C"

import (
    "fmt"
    "os"
    "time"
    "unsafe"
)

/* Object in a Ceph pool.
 *
 * Note on Ceph IO context: Object stores its last-used context for
 * filesystem-like access, however advanced users may read/write using
 * a specific context.
 */
type sys struct {
    ctx  C.rados_ioctx_t
    pool string
}

type Object struct {
    name    string
    size    int64
    modTime time.Time

    sys
}

/* Functions Size(), Mode(), ModTime(), Sys(), IsDir() used to fulfill os.FileStat interface. */

func (o *Object) Name() string {
    return o.name
}

func (o *Object) Size() int64 {
    return o.size
}

func (o *Object) Mode() os.FileMode {
    return 0 // Currently not used by RADOS
}

func (o *Object) ModTime() time.Time {
    return o.modTime
}

func (o *Object) Sys() interface{} {
    return o.sys
}

func (o *Object) IsDir() bool {
    return false
}

func (c *Context) Create(name string) (*Object, error) {
    err := c.Put(name, make([]byte, 0))

    if err != nil {
        return nil, err
    }

    // Stat the object to fill in the object structure
    obj, err := c.Stat(name)

    if err != nil {
        return nil, err
    }

    return obj.(*Object), nil
}

func (c *Context) Open(name string) (*Object, error) {
    objInfo, err := c.Stat(name)

    if err == nil {
        // Object exists, return a handle to it.
        return objInfo.(*Object), nil
    }

    // Stat failed, attempt to create the object
    obj, err := c.Create(name)

    if err != nil {
        return nil, err
    }

    return obj, nil
}

func (c *Context) Remove(name string) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_remove(c.ctx, cname); cerr != 0 {
        return fmt.Errorf("RADOS remove: %s: %s", name, strerror(cerr))
    }

    return nil
}

func (c *Context) Truncate(name string, size int64) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_trunc(c.ctx, cname, C.uint64_t(size)); cerr != 0 {
        return fmt.Errorf("RADOS trunc: %s: %s", name, strerror(cerr))
    }

    return nil
}

func byteSliceToBuffer(data []byte) (*C.char, C.size_t) {
    if len(data) > 0 {
        return (*C.char)(unsafe.Pointer(&data[0])), C.size_t(len(data))
    } else {
        return (*C.char)(unsafe.Pointer(&data)), C.size_t(0)
    }
}

func (c *Context) Put(name string, data []byte) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    cdata, cdatalen := byteSliceToBuffer(data)

    if cerr := C.rados_write_full(c.ctx, cname, cdata, cdatalen); cerr < 0 {
        return fmt.Errorf("RADOS put %s: %s", name, strerror(cerr))
    }

    return nil
}

func (c *Context) Get(name string) ([]byte, error) {
    obj, err := c.Stat(name)

    if err != nil {
        return nil, err
    }

    if obj.Size() == 0 {
        // Return an empty slice
        return make([]byte, 0), nil
    }

    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    data := make([]byte, obj.Size())
    cdata, cdatalen := byteSliceToBuffer(data)

    if cerr := C.rados_read(c.ctx, cname, cdata, cdatalen, 0); cerr < 0 {
        return nil, fmt.Errorf("RADOS get %s: %s", name, strerror(cerr))
    }

    return data, nil
}

func (c *Context) Stat(name string) (os.FileInfo, error) {
    var csize C.uint64_t
    var ctime C.time_t
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_stat(c.ctx, cname, &csize, &ctime); cerr < 0 {
        return nil, fmt.Errorf("RADOS stat %s: %s", name, strerror(cerr))
    }

    return &Object{
        name:    name,
        size:    int64(csize),
        modTime: time.Unix(int64(ctime), int64(0)),
        sys:     sys{ctx: c.ctx, pool: c.Pool},
    }, nil
}

// func (o *Object) WriteInContext(Context *c, ...)
// func (o *Object) ReadInContext(Context *c, ...)
