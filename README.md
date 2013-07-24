# RADOS.go

RADOS.go provides Go bindings for the CEPH RADOS client library (librados).
We attempt to adhere to the style of the Go OS package as much as possible
(for example, our Object API implements the FileStat and Reader/Writer
interfaces).

## License

RADOS.go is released under the simplified (2-clause) BSD license. See the
LICENSE file.

## TODO

- Godoc (!)
- Read
- ReadAt and WriteAt; more file-like object API
- Append
- Extended attributes
- TMAP -- what should this API look like?
- Object locality controls: rados_ioctx_locator_set_key, rados_clone_range
- Pool-managed snapshot
- Client-managed snapshot -- what should this API look like?
- Pool stats
- List pools
- Pool management (create/delete)
- Object list iterator

Maybe:

- Is any RADOS test-and-set functionality exposed through librados?
- Any use for AIO or more advanced IO scheduling?

## Contributors

@joshcarter
@mrkvm

## Credits

Thanks to @vuleetu for initial inspiration.

