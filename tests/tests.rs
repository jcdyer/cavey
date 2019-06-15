use std::process::Command;
use assert_cmd::prelude::*;

#[test]
fn cli_no_args() {
    Command::cargo_bin("cavey").unwrap().assert().failure();
}
