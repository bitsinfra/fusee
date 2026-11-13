#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = fusee_core::store::scan_store(data, |_, _, _| {});
    let _ = fusee_core::store::Checkpoint::decode(data);
});
