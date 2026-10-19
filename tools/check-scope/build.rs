//! Rebuilds this crate when the build moves to another checkout that shares `target/`: see `SMELTR_CHECKOUT` in
//! `.cargo/config.toml`.

fn main() {
    println!("cargo:rerun-if-env-changed=SMELTR_CHECKOUT");
}
