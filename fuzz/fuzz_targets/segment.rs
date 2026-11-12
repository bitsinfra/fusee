#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let mut n = 0u64;
    let _ = fusee_core::segment::scan_segment(data, |_, _| n += 1);
});
