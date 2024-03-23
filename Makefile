.PHONY: dev

dev:
	maturin dev -m pymodule/Cargo.toml --release
	cargo build --bin hojo --release

build:
	maturin build -m pymodule/Cargo.toml --release
