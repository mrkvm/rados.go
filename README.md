# RADOS.go

RADOS.go provides Go bindings for the CEPH RADOS client library
([librados](http://ceph.com/docs/next/rados/api/librados/)).

We attempt to adhere to the style of the Go OS package as much as possible
(for example, our Object type implements the FileStat and ReaderAt/WriterAt
interfaces).

More information on CEPH and RADOS can be found here: http://ceph.com

## License

RADOS.go is released under the simplified (2-clause) BSD license. See the
LICENSE file.

## TODO

- More generic Reader/Writer implemenation. Track file position and provide Seek()
- Close()?
- Extended attributes
- TMAP -- what should this API look like?
- Object locality controls: rados_ioctx_locator_set_key, rados_clone_range
- Pool-managed snapshot
- Client-managed snapshot -- what should this API look like?
- Object list iterator
- Real tests for cluster/pool stats.
- Change naming of cluster stat fields to match pool stats?
- Provide additional bytes used/avail in cluster stats to match pool stats?

Maybe:

- Is any RADOS test-and-set functionality exposed through librados?
- Any use for AIO or more advanced IO scheduling?

## Contributors

- [@joshcarter](https://github.com/joshcarter)
- [@mrkvm](https://github.com/mrkvm)

## Credits

This project sponsored by [Spectra Logic, Inc.](http://spectralogic.com).

Thanks to [@vuleetu](https://github.com/vuleetu)
for [initial inspiration](https://github.com/vuleetu/gorados).
