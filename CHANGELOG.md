## [1.2.5](https://github.com/jwulf/nano-sdk-js/compare/v1.2.4...v1.2.5) (2026-08-12)


### Bug Fixes

* **rest:** default REST job-worker long-poll to 30s ([#9](https://github.com/jwulf/nano-sdk-js/issues/9)) ([02b14b6](https://github.com/jwulf/nano-sdk-js/commit/02b14b6b59c600ae2560fef1988f174b1d8ef112))

## [1.2.4](https://github.com/jwulf/nano-sdk-js/compare/v1.2.3...v1.2.4) (2026-08-12)


### Bug Fixes

* fall back to REST when the Falcon WebSocket handshake is blackholed ([#8](https://github.com/jwulf/nano-sdk-js/issues/8)) ([3773ef9](https://github.com/jwulf/nano-sdk-js/commit/3773ef980817d9ff5c91855ed672bc2ec75a4033))

## [1.2.3](https://github.com/jwulf/nano-sdk-js/compare/v1.2.2...v1.2.3) (2026-08-02)


### Bug Fixes

* **deps:** release @camunda8/orchestration-cluster-api 10.0.0-alpha.22 bump ([#7](https://github.com/jwulf/nano-sdk-js/issues/7)) ([11a25cf](https://github.com/jwulf/nano-sdk-js/commit/11a25cf73a6456e114a7c85e8941716be1ca95b6))

## [1.2.2](https://github.com/jwulf/nano-sdk-js/compare/v1.2.1...v1.2.2) (2026-08-02)


### Bug Fixes

* **deps:** bump @camunda8/orchestration-cluster-api to 10.0.0-alpha.20 ([#5](https://github.com/jwulf/nano-sdk-js/issues/5)) ([e084796](https://github.com/jwulf/nano-sdk-js/commit/e08479678a91dd0e77d55407c11a8fa479ba55a6))

## [1.2.1](https://github.com/jwulf/nano-sdk-js/compare/v1.2.0...v1.2.1) (2026-08-01)


### Bug Fixes

* **transport:** crash-resilient Falcon reconnect (backoff; no unhandled rejection) ([#4](https://github.com/jwulf/nano-sdk-js/issues/4)) ([d96e722](https://github.com/jwulf/nano-sdk-js/commit/d96e722d5674fcdc943a9cfcfc776f9e6f4a8297)), closes [#3](https://github.com/jwulf/nano-sdk-js/issues/3) [#3](https://github.com/jwulf/nano-sdk-js/issues/3)

# [1.2.0](https://github.com/jwulf/nano-sdk-js/compare/v1.1.0...v1.2.0) (2026-07-04)


### Features

* graceful Falcon fallback + CAMUNDA_FORCE_REST escape hatch (1.1.0) ([f96fdb1](https://github.com/jwulf/nano-sdk-js/commit/f96fdb13cfe0271e33b37db906142ab8b8d72b7c))

# [1.1.0](https://github.com/jwulf/nano-sdk-js/compare/v1.0.0...v1.1.0) (2026-07-04)


### Features

* embedded transport (in-process μ-nano) per ADR 0005 ([dd8569e](https://github.com/jwulf/nano-sdk-js/commit/dd8569e25c5a7b464b2a4223ddfef05d8aee66ad))

# 1.0.0 (2026-06-29)


### Features

* initial public release of @nanobpm/nano-sdk ([6f39905](https://github.com/jwulf/nano-sdk-js/commit/6f399058b003ac2bd316ff03ff26fbad09987b28))
