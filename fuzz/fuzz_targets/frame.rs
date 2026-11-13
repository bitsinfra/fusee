#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let mut d = fusee_core::frame::Decoder::new();
    for chunk in data.chunks(7) {
        d.feed(chunk);
        while let Ok(Some(_)) = d.next() {}
    }
});
