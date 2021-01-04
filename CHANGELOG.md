# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.0](https://github.com/fpagliughi/mqtt.rust.redis/compare/v0.3.0..v0.2.2) - 2021-01-04

- Updated to use Paho Rust v0.9
- Updated Rust Edition to 2018
- Persistence callback now all use `&mut self` so that we can use the Redis client in safe mode all the time.
