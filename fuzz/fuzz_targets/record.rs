#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    // Canonical decoding must never panic, and must round-trip.
    if let Ok(recs) = fusee_core::record::decode_canonical_all(data) {
        let mut out = Vec::new();
        for r in &recs {
            fusee_core::record::encode_canonical(r, &mut out);
        }
        assert_eq!(fusee_core::record::decode_canonical_all(&out).ok(), Some(recs));
    }
});
