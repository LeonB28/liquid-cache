build-x86_64:
	cargo build --release --target x86_64-unknown-linux-musl

build-docker:
	docker build -t liquid-cache-flight-server:latest .

docker-compose:
	docker-compose up -d
