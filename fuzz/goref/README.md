# goref — Go reference shim for differential fuzzing

This directory holds the Go-side bridge that exposes the upstream
`github.com/ethp2p/ethp2p` codec to the Rust fuzz harness via a CGO
`c-archive`. It is the **single exception** to the clean-room policy
defined in `port-charter` Requirement 2: the shim maintainer reads
upstream Go source to write `//export` wrappers; nobody else does.

This README is the **source of truth** for the FFI surface. The Go
shim implements it; the Rust harness consumes it via matching
`extern "C"` declarations in `../src/ffi.rs`. Any change to the surface
updates this file first, then both sides.

## Maintainer responsibilities

The named shim maintainer:

- Implements the C ABI specified in this README.
- Reads upstream Go source as required to bind protobuf
  marshal/unmarshal calls into the exported functions.
- **Does NOT contribute Rust code to crates that wrap the same
  protocol surface their shim exposes.** Specifically, no PRs touching
  `crates/ethp2p-protocol/` or `crates/ethp2p-broadcast/` while serving
  as the shim maintainer for the broadcast codec.
- Keeps the shim narrow: only the four parse-and-reencode functions
  plus the allocator-pair `goref_free`. Adding new exports requires a
  PR updating this README first.

The current shim maintainer is recorded in
[`../../CONTRIBUTING.md`](../../CONTRIBUTING.md). Rotation happens via
PR against that file.

## FFI surface

All exported functions follow a uniform contract:

```c
int  goref_<msg>_parse_and_reencode(
       const uint8_t* in_buf, size_t in_len,
       uint8_t**      out_buf, size_t* out_len);
void goref_free(uint8_t* buf);
```

### Inputs

- `in_buf`: pointer to the input byte sequence. May be null **only if
  `in_len == 0`**. The shim does not retain the pointer past the call.
- `in_len`: length in bytes. Zero is valid input (an empty message).

### Outputs (success path)

- Return value `0`.
- `*out_buf`: a heap-allocated buffer obtained via `C.malloc` in the
  shim. Length is `*out_len` bytes, containing the canonical protobuf
  re-encoding produced by `proto.Marshal` on the parsed message.
- `*out_len`: length in bytes. Zero is valid output.
- Ownership transfers to the caller. The caller **must** call
  `goref_free(*out_buf)` exactly once after consuming the bytes.

### Outputs (parse-error path)

- Return value `1` (any nonzero value is treated as parse error; `1`
  is the conventional choice).
- `*out_buf` and `*out_len` are NOT modified by the shim. The caller
  MUST NOT call `goref_free` on the parse-error path.

### `goref_free`

- Releases a buffer obtained from any `goref_<msg>_parse_and_reencode`
  success path.
- Implemented in the shim as `C.free` (the buffer was `C.malloc`-ed).
- Calling with a null pointer is a no-op.
- Calling with a pointer not obtained from this shim is undefined
  behavior.

## Exported functions

### `goref_bcast_parse_and_reencode`

- Parses `in_buf[0 .. in_len]` as a `broadcast.Bcast` protobuf message.
- On success, re-encodes via `proto.Marshal` and returns the bytes.

### `goref_sess_parse_and_reencode`

- Same contract for `broadcast.Sess`.

### `goref_selector_parse_and_reencode`

- Same contract for `protocol.Selector`.

### `goref_chunk_header_parse_and_reencode`

- Same contract for `broadcast.Chunk.Header`.

### `goref_free`

- See above.

## Memory ownership rules

```
   ┌─────────────────────────────────────────────────────────────┐
   │  Lifetime of the output buffer                              │
   ├─────────────────────────────────────────────────────────────┤
   │                                                             │
   │  shim:  msg parse → proto.Marshal → bytes (Go GC-managed)   │
   │             │                                               │
   │             ▼  C.malloc + memcpy                            │
   │         C-owned buffer  ◄── ownership transfers here        │
   │             │                                               │
   │  Rust:      ▼ from_raw_parts → copy into Vec<u8>            │
   │         goref_free(C-owned buffer)                          │
   │             │                                               │
   │             ▼                                               │
   │         freed                                                │
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
```

**Why copy via `C.malloc`?** Go's garbage collector may move or free
heap-allocated `[]byte` slices at any GC cycle. Returning a pointer
into the Go heap across the cgo boundary is undefined. The shim
therefore copies the marshaled bytes into a `C.malloc`-allocated
buffer. Rust copies that buffer into a `Vec<u8>` and immediately frees
the C buffer.

## Error semantics

- Return `0` on success, `1` on parse failure.
- "Parse failure" means `proto.Unmarshal` returned a non-nil error.
  Out-of-memory failures inside the shim (e.g. `C.malloc` returning
  null) are treated as parse error and may additionally log to stderr.
- The Rust safe wrapper translates `0` to `Some(Vec<u8>)` and any
  nonzero return to `None`.

## Building the shim (manual)

The Rust `build.rs` builds the shim automatically when the
`goref-shim` feature is enabled. To build manually for testing:

```sh
cd fuzz/goref
go build -buildmode=c-archive -o $OUT_DIR/libgoref.a .
```

The resulting `libgoref.a` plus `libgoref.h` must be placed where
`build.rs` can find them (`$OUT_DIR` for the fuzz crate).

## Example shim skeleton

This is a non-functional sketch showing the cgo `//export` syntax and
the malloc/copy pattern. The maintainer fills in actual upstream
imports and parse logic.

```go
package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
    "unsafe"

    pb "github.com/ethp2p/ethp2p/broadcast/pb"
    "google.golang.org/protobuf/proto"
)

//export goref_bcast_parse_and_reencode
func goref_bcast_parse_and_reencode(
    inBuf *C.uint8_t, inLen C.size_t,
    outBuf **C.uint8_t, outLen *C.size_t,
) C.int {
    input := C.GoBytes(unsafe.Pointer(inBuf), C.int(inLen))
    var msg pb.Bcast
    if err := proto.Unmarshal(input, &msg); err != nil {
        return 1
    }
    encoded, err := proto.Marshal(&msg)
    if err != nil {
        return 1
    }
    n := C.size_t(len(encoded))
    cBuf := C.malloc(n)
    if cBuf == nil {
        return 1
    }
    if n > 0 {
        C.memcpy(cBuf, unsafe.Pointer(&encoded[0]), n)
    }
    *outBuf = (*C.uint8_t)(cBuf)
    *outLen = n
    return 0
}

//export goref_free
func goref_free(buf *C.uint8_t) {
    if buf != nil {
        C.free(unsafe.Pointer(buf))
    }
}

func main() {} // required by cgo, never called
```

The other three message-type functions follow the same pattern with
the corresponding upstream package and protobuf type.

## Validating the shim

Once the shim source lands:

```sh
# Builds libgoref.a via build.rs invocation of go build.
cd fuzz
cargo fuzz build --features goref-shim

# Smoke run; libfuzzer + cgo + Go runtime should coexist.
cargo fuzz run codec_bcast_diff --features goref-shim -- -max_total_time=60
```

If libfuzzer's signal handlers fight with Go's runtime (symptoms:
hangs at shutdown, segfaults during fuzzing), see the design doc
under the archived slice 2 spec for the honggfuzz fallback path.
