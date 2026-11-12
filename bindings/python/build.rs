fn main() {
    // macOS: leave Python symbols to be resolved by the interpreter that
    // loads the extension (maturin does the same).
    pyo3_build_config::add_extension_module_link_args();
}
