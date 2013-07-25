package rados

import (
    "bytes"
    "fmt"
    "io"
    "os"
    "testing"
    "time"
)

func errorOnError(t *testing.T, e error, message string, parameters ...interface{}) {
    if e != nil {
        t.Errorf("%v : %v", e, fmt.Sprintf(message, parameters...))
    }
}

func fatalOnError(t *testing.T, e error, message string, parameters ...interface{}) {
    if e != nil {
        t.Fatalf("%v : %v", e, fmt.Sprintf(message, parameters...))
    }
}

func poolName() string {
    return fmt.Sprintf("rados.go.test.%d.%d", time.Now().Unix(), os.Getpid())
}

type radosTest struct {
    t        *testing.T
    rados    *Rados
    poolName string
}

func setup(t *testing.T) *radosTest {
    var rados *Rados
    var err error

    rados, err = NewDefault()
    fatalOnError(t, err, "Setup: New")

    poolName := poolName()
    err = rados.PoolCreate(poolName)
    fatalOnError(t, err, "Setup: PoolCreate")

    return &radosTest{
        rados:    rados,
        poolName: poolName,
    }
}

func teardown(t *testing.T, test *radosTest) {
    var err error

    err = test.rados.PoolDelete(test.poolName)
    fatalOnError(t, err, "Teardown: PoolDelete")

    err = test.rados.Release()
    fatalOnError(t, err, "Teardown: Release")
}

func Test_RadosNew(t *testing.T) {
    var rados *Rados
    var err error

    rados, err = NewDefault()
    fatalOnError(t, err, "New")

    err = rados.Release()
    fatalOnError(t, err, "Release")

    if rados, err = New("path that does not exist"); err == nil {
        t.Errorf("New should have failed")
        rados.Release()
    }
}

func Test_RadosPoolCreateDelete(t *testing.T) {
    var rados *Rados
    var err error

    rados, err = NewDefault()
    fatalOnError(t, err, "New")
    defer rados.Release()

    poolName := poolName()
    err = rados.PoolCreate(poolName)
    fatalOnError(t, err, "PoolCreate")

    err = rados.PoolDelete(poolName)
    fatalOnError(t, err, "PoolDelete")
}

func Test_RadosContext(t *testing.T) {
    test := setup(t)
    defer teardown(t, test)

    ctx, err := test.rados.NewContext(test.poolName)
    fatalOnError(t, err, "NewContext")
    ctx.Release()

    if ctx, err = test.rados.NewContext("pool that does not exist"); err == nil {
        t.Errorf("NewContext should have failed")
        ctx.Release()
    }
}

// Test basic object operations.
func Test_RadosObject(t *testing.T) {
    test := setup(t)
    defer teardown(t, test)

    ctx, err := test.rados.NewContext(test.poolName)
    fatalOnError(t, err, "NewContext")
    defer ctx.Release()

    name := "test-object"
    name2 := "test-object2"
    data := []byte("test data")

    // Create an object
    _, err = ctx.Create(name)
    fatalOnError(t, err, "Create")

    // Make sure it's there
    objInfo, err := ctx.Stat(name)
    fatalOnError(t, err, "Stat")

    if objInfo.Size() != int64(0) {
        t.Errorf("Object size mismatch, was %s, expected %s", objInfo.Size(), 0)
    }

    // Put data in the object
    err = ctx.Put(name, data)
    fatalOnError(t, err, "Put")

    // Make sure everything looks right
    objInfo, err = ctx.Stat(name)
    fatalOnError(t, err, "Stat")

    if objInfo.Name() != name {
        t.Errorf("Object name mismatch, was %s, expected %s", objInfo.Name(), name)
    }

    if objInfo.Size() != int64(len(data)) {
        t.Errorf("Object size mismatch, was %d, expected %d", objInfo.Size(), len(data))
    }

    // Get the data back
    data2, err := ctx.Get(name)
    fatalOnError(t, err, "Get")

    // It better be the same
    if !bytes.Equal(data, data2) {
        t.Errorf("Object data mismatch, was %s, expected %s", data2, data)
    }

    // Open an existing object
    obj, err := ctx.Open(name)
    fatalOnError(t, err, "Open")

    // Make sure everything looks right
    if obj.Name() != name {
        t.Errorf("Object name mismatch, was %s, expected %s", obj.Name(), name)
    }

    if obj.Size() != int64(len(data)) {
        t.Errorf("Object size mismatch, was %d, expected %d", obj.Size(), len(data))
    }

    // Open a new object
    obj, err = ctx.Open(name2)
    fatalOnError(t, err, "Open")

    // Make sure it's there
    objInfo, err = ctx.Stat(name2)
    fatalOnError(t, err, "Stat")

    if objInfo.Size() != int64(0) {
        t.Errorf("Object size mismatch, was %d, expected %d", objInfo.Size(), 0)
    }

    // Remove the objects
    err = ctx.Remove(name)
    errorOnError(t, err, "Remove")
    err = ctx.Remove(name2)
    errorOnError(t, err, "Remove")

    // They should be gone
    objInfo, err = ctx.Stat(name)
    if err == nil {
        t.Errorf("Object %s should have been deleted be status returned success", name)
    }

    objInfo, err = ctx.Stat(name2)
    if err == nil {
        t.Errorf("Object %s should have been deleted be status returned success", name2)
    }
}

func Test_ReadAtWriteAt(t *testing.T) {
    test := setup(t)
    defer teardown(t, test)

    ctx, err := test.rados.NewContext(test.poolName)
    fatalOnError(t, err, "NewContext")
    defer ctx.Release()

    name := "test-object"
    data := make([]byte, 5)

    // Create a new object
    obj, err := ctx.Create(name)
    fatalOnError(t, err, "Create")

    // Try to Read the first byte (expect EOF).
    n, err := obj.ReadAt(data, 0)

    if err == nil {
        t.Errorf("Expected non-nil error on ReadAt() of empty object.")
    }

    if err != io.EOF {
        t.Errorf("Expected EOF for ReadAt() of empty object, got %s", err)
    }

    if n != 0 {
        t.Errorf("Expected 0 bytes read for ReadAt() of empty object, got %d", n)
    }

    // Write some data to the beginning
    data = []byte("12345")
    n, err = obj.WriteAt(data, 0)
    fatalOnError(t, err, "WriteAt")

    if n != len(data) {
        t.Errorf("Expected to have %d bytes written but was %d", len(data), n)
    }

    // Read the third byte
    data = make([]byte, 1)
    n, err = obj.ReadAt(data, 2)
    fatalOnError(t, err, "ReadAt")

    if n != len(data) {
        t.Errorf("Expected to have %d bytes read but was %d", len(data), n)
    }

    if data[0] != '3' {
        t.Errorf("Expected to have read 3 but was %v", data[0])
    }

    // Write the third byte with something new
    data[0] = 'C'
    n, err = obj.WriteAt(data, 2)
    fatalOnError(t, err, "WriteAt")

    if n != len(data) {
        t.Errorf("Expected to have %d bytes written but was %d", len(data), n)
    }

    // Make sure it's correct
    data = make([]byte, 1)
    n, err = obj.ReadAt(data, 2)
    fatalOnError(t, err, "ReadAt")

    if n != len(data) {
        t.Errorf("Expected to have %d bytes read but was %d", len(data), n)
    }

    if data[0] != 'C' {
        t.Errorf("Expected to have read C but was %v", data[0])
    }

    // Try to read past the end
    data = make([]byte, 2)
    n, err = obj.ReadAt(data, 4)

    if err == nil {
        t.Errorf("Expected non-nil error on ReadAt() reading past end of object")
    }

    if err != io.EOF {
        t.Errorf("Expected EOF for ReadAt() reading past end of object, got %s", err)
    }

    if n != 1 {
        t.Errorf("Expected 1 bytes read for ReadAt() past end of object, got %d", n)
    }

    if data[0] != '5' {
        t.Errorf("Expected to have read 5 but was %v", data[0])
    }

    // Try to write past the end
    data[0] = 'E'
    data[1] = 'F'
    n, err = obj.WriteAt(data, 4)
    fatalOnError(t, err, "WriteAt")

    if n != len(data) {
        t.Errorf("Expected to have %d bytes written but was %d", len(data), n)
    }

    // Read the whole object and make sure the data is correct
    data = []byte("12C4EF")
    data2, err := ctx.Get(name)
    fatalOnError(t, err, "Get")

    // It better be the same
    if !bytes.Equal(data, data2) {
        t.Errorf("Object data mismatch, was %s, expected %s", data2, data)
    }
}
