//! Safe wrappers around the `goref/` shim's C ABI.
//!
//! The `extern "C"` block and all `unsafe` blocks live here and only
//! here. The contract this module mirrors is documented in
//! `../goref/README.md` — that file is the source of truth for both
//! the Go shim and the declarations below.
//!
//! When the `goref-shim` feature is disabled, this module is empty
//! (the `#[cfg]` gate hides everything). Without the shim the
//! differential fuzz targets are absent and the sanity targets work
//! unchanged.

#![allow(unsafe_code)]

#[cfg(feature = "goref-shim")]
mod with_shim {
    use std::ffi::c_int;
    use std::os::raw::c_void;

    extern "C" {
        fn goref_bcast_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_sess_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_selector_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_chunk_header_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_rs_preamble_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_rs_chunk_ident_parse_and_reencode(
            in_buf: *const u8,
            in_len: usize,
            out_buf: *mut *mut u8,
            out_len: *mut usize,
        ) -> c_int;

        fn goref_free(buf: *mut u8);
    }

    /// Calls a `goref_<msg>_parse_and_reencode` function and translates
    /// the C-allocated output into a Rust-owned `Vec<u8>`.
    ///
    /// Returns `Some(bytes)` on success (return code 0) and `None` on
    /// parse error (any nonzero return code).
    fn invoke(
        f: unsafe extern "C" fn(*const u8, usize, *mut *mut u8, *mut usize) -> c_int,
        input: &[u8],
    ) -> Option<Vec<u8>> {
        let mut out_buf: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;

        // SAFETY:
        // * `input.as_ptr()` is valid for `input.len()` bytes (Rust slice
        //   guarantees) and lives at least until the call returns.
        // * `&mut out_buf` and `&mut out_len` are valid pointers to
        //   stack-local variables.
        // * The C contract (goref/README.md) guarantees that on a
        //   nonzero return, `out_buf` and `out_len` are not modified;
        //   on a zero return, `out_buf` is a `C.malloc`-ed buffer of
        //   exactly `out_len` bytes that we own and must free via
        //   `goref_free`.
        let rc = unsafe { f(input.as_ptr(), input.len(), &mut out_buf, &mut out_len) };

        if rc != 0 {
            return None;
        }

        // SAFETY:
        // * On a zero return code, `out_buf` is non-null and points to
        //   a contiguous `out_len`-byte allocation. (Special case:
        //   `out_len == 0` is allowed; we treat as empty.)
        // * The bytes are initialized by the shim before return.
        // * We immediately copy into a Vec<u8> and then call
        //   `goref_free`, transferring no aliasing back across the
        //   boundary.
        let copied: Vec<u8> = if out_len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(out_buf as *const u8, out_len) }.to_vec()
        };

        // SAFETY:
        // * `out_buf` was returned by the shim's `C.malloc`, so freeing
        //   via `goref_free` (a C.free wrapper) is the matching
        //   allocator pair.
        // * Calling with a null pointer is documented as a no-op, but
        //   in the success path the shim guarantees non-null.
        unsafe { goref_free(out_buf) };

        // Defeat unused-but-valid: confirm the output slot was a `*mut c_void`-shaped pointer.
        let _ = std::mem::size_of::<*mut c_void>();

        Some(copied)
    }

    /// Parse and re-encode a `broadcast.Bcast` message via the Go shim.
    /// Returns `None` if the input does not parse as `Bcast`.
    #[must_use]
    pub fn bcast_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_bcast_parse_and_reencode, input)
    }

    /// Parse and re-encode a `broadcast.Sess` message.
    #[must_use]
    pub fn sess_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_sess_parse_and_reencode, input)
    }

    /// Parse and re-encode a `protocol.Selector` message.
    #[must_use]
    pub fn selector_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_selector_parse_and_reencode, input)
    }

    /// Parse and re-encode a `broadcast.Chunk.Header` message.
    #[must_use]
    pub fn chunk_header_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_chunk_header_parse_and_reencode, input)
    }

    /// Parse and re-encode a `broadcast.rs.Preamble` message.
    #[must_use]
    pub fn rs_preamble_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_rs_preamble_parse_and_reencode, input)
    }

    /// Parse and re-encode a `broadcast.rs.ChunkIdent` message.
    #[must_use]
    pub fn rs_chunk_ident_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
        invoke(goref_rs_chunk_ident_parse_and_reencode, input)
    }
}

#[cfg(feature = "goref-shim")]
pub use with_shim::*;
