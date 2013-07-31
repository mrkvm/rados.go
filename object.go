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
    "io"
    "os"
    "time"
    "unsafe"
)

// sys contains underlying RADOS IO context and pool information for an object.
// Needed for FileStat interface.
type sys struct {
    c    *Context
    pool string
}

// Object represents an object in RADOS pool.
//
// Note on RADOS IO context: Object stores its last-used context for
// filesystem-like access, however advanced users may read/write using
// a specific context.
type Object struct {
    name    string
    size    int64
    modTime time.Time

    sys
}

// Functions Size(), Mode(), ModTime(), Sys(), IsDir() used to fulfill os.FileStat interface.

// Name returns the name of the given object.
func (o *Object) Name() string {
    return o.name
}

// Name returns the size in bytes of the given object.
func (o *Object) Size() int64 {
    return o.size
}

// Mode returns the file mode of the given object.
//
// NOTE: not currently used.
func (o *Object) Mode() os.FileMode {
    return 0 // Currently not used by RADOS
}

//Name returns the size in bytes of the given object.
func (o *Object) ModTime() time.Time {
    return o.modTime
}

// Sys returns underlying RADOS information for the given object (context, pool name).
func (o *Object) Sys() interface{} {
    return o.sys
}

// IsDir would return true if the given object represents a directory.
// However, RADOS has no notion of directories, so we always return false here.
func (o *Object) IsDir() bool {
    return false
}

// Create creates an empty RADOS object in the pool referenced by the given
// context and returns a handle to it.
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

// Open returns a handle to the named object in the pool referenced by the given
// context. An empty object will be created if it doesn't already exist.
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

// Stat retrieves information about the named object in the pool referenced
// by the given context. A pointer to the object is returned as an
// os.FileInfo (fulfills FileStat interface).
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
        sys:     sys{c: c, pool: c.Pool},
    }, nil
}

// Remove deletes the named object in the pool referenced by the given context.
func (c *Context) Remove(name string) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_remove(c.ctx, cname); cerr != 0 {
        return fmt.Errorf("RADOS remove: %s: %s", name, strerror(cerr))
    }

    return nil
}

// Truncate sets the size of the named object in the pool referenced by
// the given context to size. If this enlarges the object, the new area
// is logically filled with zeroes. If this shrinks the object, the data
// is removed.
func (c *Context) Truncate(name string, size int64) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    if cerr := C.rados_trunc(c.ctx, cname, C.uint64_t(size)); cerr != 0 {
        return fmt.Errorf("RADOS trunc: %s: %s", name, strerror(cerr))
    }

    return nil
}

// Append writes the given data to the end of the named object
// in the pool referenced by the given context.
func (c *Context) Append(name string, data []byte) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    cdata, cdatalen := byteSliceToBuffer(data)

    if cerr := C.rados_append(c.ctx, cname, cdata, cdatalen); cerr < 0 {
        return fmt.Errorf("RADOS put %s: %s", name, strerror(cerr))
    }

    return nil
}

// Get reads all the data in the named object in the pool referenced by
// the given context. The data is returned as a byte slice.
//
// If the object does not exist, an error is returned.
// If the object contains no data, an empty slice is returned.
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

// Put writes data to the named object in the pool referenced by the
// given context. If the object does not exist, it will be created.
// If the object exists, it will first be truncated to 0 then overwritten.
func (c *Context) Put(name string, data []byte) error {
    cname := C.CString(name)
    defer C.free(unsafe.Pointer(cname))

    cdata, cdatalen := byteSliceToBuffer(data)

    if cerr := C.rados_write_full(c.ctx, cname, cdata, cdatalen); cerr < 0 {
        return fmt.Errorf("RADOS put %s: %s", name, strerror(cerr))
    }

    return nil
}

// Stat wrap the Context-based Stat function for the given object.
// The object structure is modified in place
func (o *Object) Stat() error {
    objInfo, err := o.c.Stat(o.name)

    if err != nil {
        return err
    }

    obj := objInfo.(*Object)
    o.size = obj.size
    o.modTime = obj.modTime

    return nil
}

// Remove wraps the Context-based Remove function for the given object.
func (o *Object) Remove() error {
    return o.c.Remove(o.name)
}

// Truncate wraps the Context-based Truncate function for the given object.
func (o *Object) Truncate(size int64) error {
    return o.c.Truncate(o.name, size)
}

// Append wraps the Context-based Append function for the given object.
func (o *Object) Append(data []byte) error {
    return o.c.Append(o.name, data)
}

// Get wraps the Context-based Get function for the given object.
func (o *Object) Get() ([]byte, error) {
    return o.c.Get(o.name)
}

// Put wraps the Context-based Put function for the given object.
func (o *Object) Put(data []byte) error {
    return o.c.Put(o.name, data)
}

// ReadAt reads len(data) bytes from the given RADOS object at the byte
// offset off. It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(data).
// At the end of file, that error is io.EOF.
//
// This function adopted from the Go os.ReadAt() function.
func (o *Object) ReadAt(data []byte, off int64) (n int, err error) {
    cname := C.CString(o.name)
    defer C.free(unsafe.Pointer(cname))

    for len(data) > 0 {
        cdata, cdatalen := byteSliceToBuffer(data)
        coff := C.uint64_t(off)

        cerr := C.rados_read(o.c.ctx, cname, cdata, cdatalen, coff)

        if cerr == 0 {
            return n, io.EOF
        }

        if cerr < 0 {
            err = fmt.Errorf("RADOS read %s: %s", o.name, strerror(cerr))
            break
        }

        n += int(cerr)
        data = data[cerr:]
        off += int64(cerr)
    }

    return
}

// WriteAt writes len(data) bytes to the RADOS object at the byte offset
// off. It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n < len(data).
func (o *Object) WriteAt(data []byte, off int64) (n int, err error) {
    cname := C.CString(o.name)
    defer C.free(unsafe.Pointer(cname))

    for len(data) > 0 {
        cdata, cdatalen := byteSliceToBuffer(data)
        coff := C.uint64_t(off)

        cerr := C.rados_write(o.c.ctx, cname, cdata, cdatalen, coff)

        if cerr < 0 {
            err = fmt.Errorf("RADOS write %s: %s", o.name, strerror(cerr))
            break
        }

        n += int(cerr)
        data = data[cerr:]
        off += int64(cerr)
    }

    return
}

// TODO:
// func (o *Object) WriteInContext(Context *c, ...)
// func (o *Object) ReadInContext(Context *c, ...)
