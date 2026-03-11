## [1.1.0-beta.11](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.10...v1.1.0-beta.11) (2026-03-11)


### Bug Fixes

* **ci:** update gitops workflow parameters to match shared workflow v1.15.0 ([66ed158](https://github.com/LerianStudio/matcher/commit/66ed1584b52d5a4e98395f3a0af1328bf632d861))

## [1.1.0-beta.10](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.9...v1.1.0-beta.10) (2026-03-09)


### Features

* **configuration:** add FETCHER source type and regenerate OpenAPI specs ([a96dae8](https://github.com/LerianStudio/matcher/commit/a96dae83133847ca69b1f05e3645949c80d3a7f4))
* **ci:** changelog ([8ef8d41](https://github.com/LerianStudio/matcher/commit/8ef8d41af905c2789211dbb20c5ffd28b82ecc5a))


### Bug Fixes

* **lint:** exclude gosec G118 false-positives for cross-function cancel propagation ([3e714ba](https://github.com/LerianStudio/matcher/commit/3e714ba76b6fbdfc2223d73bd5029758dbb5cb88))
* regenerate broken mocks for type-aliased interfaces and correct nosec directive ([e261521](https://github.com/LerianStudio/matcher/commit/e2615212eb1c8322ead879a364e0ebc9395ddcd3))
* resolve data race on productionMode and remove stale nolint:gosec directives ([6045dd1](https://github.com/LerianStudio/matcher/commit/6045dd16d36aeec5e664ee13bf60ac53c388debd))
* **lint:** revert nosec G107 to G704 and scope G118 exclusions to specific files ([b79833d](https://github.com/LerianStudio/matcher/commit/b79833dc8c236c1efb8b9bfb6951ec2843136012))
* **governance:** truncate test timestamps to microsecond precision for sqlmock ([14b8cd1](https://github.com/LerianStudio/matcher/commit/14b8cd19e6db45a6b0504ceb81f13d9a7621a4f7))

## [1.1.0-beta.9](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.8...v1.1.0-beta.9) (2026-03-05)


### Bug Fixes

* **migration:** add idempotent fee normalization column guard ([4f144db](https://github.com/LerianStudio/matcher/commit/4f144db3582687fe789f8ed61b7065feb10a3e2e))

## [1.1.0-beta.8](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.7...v1.1.0-beta.8) (2026-03-03)

## [1.1.0-beta.7](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.6...v1.1.0-beta.7) (2026-03-02)

## [1.1.0-beta.6](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.5...v1.1.0-beta.6) (2026-02-26)


### Features

* **rabbitmq:** add insecure health check policy with multi-layer validation ([cd6f4cb](https://github.com/LerianStudio/matcher/commit/cd6f4cbbefed15777f77d6ac33c2350666df036e))

## [1.1.0-beta.5](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.4...v1.1.0-beta.5) (2026-02-25)


### Features

* remove SSL/TLS and AUTH_ENABLED production validations ([022cf2b](https://github.com/LerianStudio/matcher/commit/022cf2bf6e98e73f8d0a2849419cb6a2c93a7f21))

## [1.1.0-beta.4](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.3...v1.1.0-beta.4) (2026-02-25)


### Bug Fixes

* run go mod vendor in Dockerfile before build ([24253d1](https://github.com/LerianStudio/matcher/commit/24253d12322c908b5733fa343032dbdaab9babd0))
* update gofiber to v2.52.12 (CVE-2026-25882) ([c475bab](https://github.com/LerianStudio/matcher/commit/c475bab41e28389cc9dc01a44515bdce319275bf))
* use embedded migrations to eliminate filesystem access ([d25627e](https://github.com/LerianStudio/matcher/commit/d25627e87080d4f42387291a2d19c44f27188dd5))

## [1.1.0-beta.3](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.2...v1.1.0-beta.3) (2026-02-25)

## [1.1.0-beta.2](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.1...v1.1.0-beta.2) (2026-02-24)


### Performance Improvements

* **air:** enable exclude_unchanged to improve live-reload performance ([d612be7](https://github.com/LerianStudio/matcher/commit/d612be7675e79281d05acbdfc681555e6df680bd))

## [1.1.0-beta.1](https://github.com/LerianStudio/matcher/compare/v1.0.0...v1.1.0-beta.1) (2026-02-21)


### Features

* **reporting:** expand export report types with EXCEPTIONS and aliases ([d92dab2](https://github.com/LerianStudio/matcher/commit/d92dab2e5457f7c733607f6a715e116bdaed14a8))
* **configuration:** support inline source and rule creation in CreateContext ([7038403](https://github.com/LerianStudio/matcher/commit/7038403ccf8a294e36bd67ec256dc7f0aa62342d))


### Bug Fixes

* **integration:** add RabbitMQ startup retry and remove test parallelism race ([4568b06](https://github.com/LerianStudio/matcher/commit/4568b06f3b15b7cf42649c6e56e5a4e77aed9264))
* **bootstrap:** group var declarations for style consistency ([b859319](https://github.com/LerianStudio/matcher/commit/b859319e62bdba6f6bc97a2f9108aa96ff3b5cc9))
* **e2e:** resolve dashboard stresser flakiness with unique names ([7e93cfc](https://github.com/LerianStudio/matcher/commit/7e93cfcc13d9d074eceebe09b39e9f8b7f7f42ed))

## 1.0.0 (2026-02-19)
