During transfer the data will be piped from the source to the target.

Its possible to add certain operations to this pipe.

## Transfer buffering


The `--buffer` option might help since it acts as an IO buffer: zfs send can vary wildly between completely idle and huge bursts of data. When zfs send is idle, the buffer will continue transferring data to the other side.

This needs the mbuffer command on both sides.

You can tune the chunk size that mbuffer uses with `--buffer-chunk-size`. (version 4.0 and higher)
## Compression

If you're transferring over a slow link it might be useful to use `--compress`. This will compress the data before sending, so it uses less bandwidth. 

An alternative to this is to use `--zfs-compressed`: This will transfer blocks that already have compression intact. 

* `--compress` will usually compress much better but uses much more resources.
* ` --zfs-compressed` uses the least resources, but can be a disadvantage if you want to use a different compression method on the target.

Dont use both options at the same time, since its probably wont help.

By default `--compress` uses pigz-fast. Use `--compress=...` to select a specific compressor:

* `pigz-fast`: Uses pigz -3
* `pigz-slow`: Uses pigz -9
* `gzip`: Uses gzip -3 and zcat.
* `zstd-fast`: Uses zstdmt -3
* `zstd-slow`: Uses zstdmt -19
* `zstd-adapt`: Uses zstdmt --adapt
* `xz`: Uses xz
* `lzo`: Uses lzop
* `lz4`: Uses lz4

Offcourse the specific compressors need to be installed on both sides.
## Rate limiting

If you want to limit the datarate, try using the `--rate` option. This is usefull to not saturate a slow uplink or do reduce IO load.

This needs the mbuffer command on the sending side.

## Custom pipes

It's also possible to add custom send or receive pipes with `--send-pipe` and `--recv-pipe`.

This way you can pipe the data through and custom compressor or command you like.

## Putting it all together

These options all work together, when all options are active:

Pipe on the the sending side:

```
zfs send | buffer | custom send pipes | compression | transfer rate limiter | ssh
```

On the receiving side:
```
decompression | custom recv pipes | buffer | zfs recv
```

The buffer on the receiving side is only added if its on a different host.

Also zfs-autobackup will warn you if you do something useless, like using --compress for local transfers on the same host.
