#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(input) = std::str::from_utf8(data) {
        // parse_all should never panic, only return Ok or Err
        let _ = gqlite_parser::Parser::parse_all(input);
    }
});
