## [1.1.0-beta.14](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.13...v1.1.0-beta.14) (2026-03-13)

## [1.1.0-beta.13](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.12...v1.1.0-beta.13) (2026-03-12)


### Features

* **bootstrap:** add fetcher config defaults and validation ([fe3a02b](https://github.com/LerianStudio/matcher/commit/fe3a02b364fe341a2c7c2545d45b0d4b881fde5b))
* add fetcher integration and discovery bounded context ([5daf5ce](https://github.com/LerianStudio/matcher/commit/5daf5ce1b9f3f1af25ebaa3a021f98e9e1325212))
* **discovery:** add transactional conditional update for extraction ([c61d200](https://github.com/LerianStudio/matcher/commit/c61d2008750123592a792712a7b1d9bbf52db7a7))
* **discovery:** ship extraction workflow and harden discovery sync ([a2784f5](https://github.com/LerianStudio/matcher/commit/a2784f5a3791c424ff5b6933de76b2069f9f3c74))
* **bootstrap:** wire config manager and worker manager into service lifecycle ([66e1f17](https://github.com/LerianStudio/matcher/commit/66e1f17b79611a1c7561283e94fffb0b3c175108))


### Bug Fixes

* **bootstrap:** harden config API, audit, worker factories, and schema ([211b327](https://github.com/LerianStudio/matcher/commit/211b32777d32ff482e01e401b2641eefea28c630))
* **discovery:** harden fetcher lifecycle and migration safety ([79f9662](https://github.com/LerianStudio/matcher/commit/79f9662746c8879d83ff7bbbccbb2a687010b8c0))
* **discovery:** harden fetcher lifecycle and startup safety ([7db31bc](https://github.com/LerianStudio/matcher/commit/7db31bc3d9fff369ade400542e7dd522217cf313))
* **bootstrap:** harden runtime config lifecycle ([0efe6b1](https://github.com/LerianStudio/matcher/commit/0efe6b1e24375ef82af2044d9755e2da9fec2ee2))
* **bootstrap:** harden runtime config lifecycle ([f2d01d8](https://github.com/LerianStudio/matcher/commit/f2d01d8c7b8389c5294cb83acdf922052e6df748))
* **bootstrap:** harden runtime config updates ([b5dca5b](https://github.com/LerianStudio/matcher/commit/b5dca5bc6addaaf40574a472bbb75dfb9e3cfefc))
* **bootstrap:** remove stale mutable config keys and align test signature ([2e428ea](https://github.com/LerianStudio/matcher/commit/2e428ea8acd6846132fbda4eb020be128b3af999))
* **bootstrap:** resolve runtime config merge conflicts ([80564f7](https://github.com/LerianStudio/matcher/commit/80564f7e9b6cdae80b0403cbb63de4fe252f0222))
* **swagger:** restore API error schemas for generated docs ([a991db9](https://github.com/LerianStudio/matcher/commit/a991db943d4762b6dc0b400313aa658e33db7112))
* **bootstrap:** tighten runtime worker orchestration ([729d1a2](https://github.com/LerianStudio/matcher/commit/729d1a2d2a6ed820bdbf920f0e037416cf02f076))

## [1.1.0-beta.12](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.11...v1.1.0-beta.12) (2026-03-12)


### Features

* **bootstrap:** add fetcher config defaults and validation ([eb6dced](https://github.com/LerianStudio/matcher/commit/eb6dced0309ccb45daec210bc764ab7d83c5f3e0))
* **workers:** add UpdateRuntimeConfig and safe channel reset for hot-reload ([5a6d14e](https://github.com/LerianStudio/matcher/commit/5a6d14e481dc3dc4aec119afd06caccadf30c638))
* **deps:** add viper for configuration management ([d3c8a06](https://github.com/LerianStudio/matcher/commit/d3c8a06f6a4b7daaba036c1218548ebe7725a799))
* add YAML config support with hot-reload and runtime config API ([7fdc698](https://github.com/LerianStudio/matcher/commit/7fdc69849c249a49880bf0fa01aea6ba388b7053))
* **bootstrap:** wire config manager and worker manager into service lifecycle ([bb21dec](https://github.com/LerianStudio/matcher/commit/bb21dece3d4e841df63a92a29947b3ab9663cbdc))
* **bootstrap:** wire dynamic configGetter, config history API, and service lifecycle ([9e968f5](https://github.com/LerianStudio/matcher/commit/9e968f511f92b4c47b82403d0cd1fe6ce8493884))
* **bootstrap:** wire hot worker config, dynamic rate-limit expiry, and auth-gated config API ([469e3dc](https://github.com/LerianStudio/matcher/commit/469e3dc2c162d836c75746cc5a3deaad6ac51f18))


### Bug Fixes

* **bootstrap:** add configurable health-check timeout and document shutdown ordering ([6ef8732](https://github.com/LerianStudio/matcher/commit/6ef8732785fdafff608419df3d8698a10eeaf306))
* **bootstrap:** add HealthCheckTimeoutSec to defaultConfig and bindDefaults ([2e94aa9](https://github.com/LerianStudio/matcher/commit/2e94aa9ec4c0c65873031c9811f062134cf5b6a1))
* **governance:** add normalizeArchivalWorkerConfig for defense-in-depth ([a667547](https://github.com/LerianStudio/matcher/commit/a6675475ffba014b69bf2e7912842e67ceb3fbdd))
* **bootstrap:** always apply runtime config before worker restart ([5776d12](https://github.com/LerianStudio/matcher/commit/5776d1285e8bb6ce2c08c957e31af0e4a3c5f5d2))
* **bootstrap:** compute field-level diffs in config change tracking ([5684537](https://github.com/LerianStudio/matcher/commit/568453768f2eb647661579878b9e5e1490bc32f0))
* correct test indentation and add nosec annotations ([2336bf8](https://github.com/LerianStudio/matcher/commit/2336bf81dbfa3e9390adc0cd2a07f734bd247699))
* **bootstrap:** derive SafeError production flag and forward validation errors ([312b9d1](https://github.com/LerianStudio/matcher/commit/312b9d1c522c3acfb960ea07a6bdc6512f0f5859))
* **bootstrap:** enforce rate-limit bounds and remove dead fetcher durations ([a6c1b22](https://github.com/LerianStudio/matcher/commit/a6c1b220a3e73ff12821dd0d03e2f4769cda8cd1))
* **bootstrap:** fix typo in fetcher extraction timeout constant name ([4741437](https://github.com/LerianStudio/matcher/commit/47414374727d7f6db0366a8b40553918449d06a0))
* **bootstrap:** handle env-overridden updates and idempotent watcher startup ([0872082](https://github.com/LerianStudio/matcher/commit/08720826bf15751737f4913709f1f73b3f0563c1))
* **bootstrap:** harden config API audit context and env override visibility ([6276538](https://github.com/LerianStudio/matcher/commit/6276538188f544c8726117d206050841a04cedbd))
* **bootstrap:** harden config API auth, tracer fallback, and manager lifecycle ([bcfecc0](https://github.com/LerianStudio/matcher/commit/bcfecc0483f26ddd5ff587bc743ee96d01df17bc))
* **bootstrap:** harden config API, audit, worker factories, and schema ([25e1709](https://github.com/LerianStudio/matcher/commit/25e17093e405dc616eb4271433dfe8915a432f5a))
* **bootstrap:** harden config file path resolution against traversal ([fc4deaa](https://github.com/LerianStudio/matcher/commit/fc4deaa2ed76281fc1c1cd9855d58dfe2ff0f2c8))
* **bootstrap:** harden config manager subscriber lifecycle and source tagging ([f0f5cba](https://github.com/LerianStudio/matcher/commit/f0f5cbac100df2d24fd0889284fe2b4930d66704))
* **bootstrap:** harden config manager with nil guards and type validation ([2751348](https://github.com/LerianStudio/matcher/commit/275134899205b6dc85fc1627fcc8a4b484601f97))
* **bootstrap:** harden config reload lifecycle and nil guards ([aa76163](https://github.com/LerianStudio/matcher/commit/aa761631166f91972ceceaa1ac88a3cf523cad97))
* **bootstrap:** harden runtime config lifecycle ([bbd35b7](https://github.com/LerianStudio/matcher/commit/bbd35b70d1c312183c9d8e773823d60659de5ea7))
* **bootstrap:** harden runtime config lifecycle ([c10c671](https://github.com/LerianStudio/matcher/commit/c10c67131ec28cee622b2392bfb32139483c4036))
* **bootstrap:** harden runtime config updates ([f089042](https://github.com/LerianStudio/matcher/commit/f089042b145771066bda539b1eef4004b2b9a646))
* **bootstrap:** harden subscriber lifecycle, path validation, and atomic write ([1f356ec](https://github.com/LerianStudio/matcher/commit/1f356ece2e41a5b02a09512fe8983110ada25fff))
* **makefile:** isolate unit tests from host environment variables ([3c57128](https://github.com/LerianStudio/matcher/commit/3c571288ee75e084c2dcea4f4cd352ffde5e25fc))
* **bootstrap:** make worker restarts rollback-safe and dynamic-aware ([a3f716f](https://github.com/LerianStudio/matcher/commit/a3f716f2f246bbf9274b2cfad1cd1a081b85574b))
* **bootstrap:** preserve env-explicit zero overrides and redact URI credentials ([7874c6e](https://github.com/LerianStudio/matcher/commit/7874c6ecbc1655ab7b2ea44977823ac033f66b95))
* **bootstrap:** redact sensitive values in config audit change maps ([f66d07f](https://github.com/LerianStudio/matcher/commit/f66d07f21885c5682e023cd9be09dec4c0e6fa49))
* **bootstrap:** set proxy header when trusted proxies are configured ([dcfcf0c](https://github.com/LerianStudio/matcher/commit/dcfcf0c61a2592be2e4f7e9916a39957a0d9747a))
* **bootstrap:** tighten runtime worker orchestration ([78f4c59](https://github.com/LerianStudio/matcher/commit/78f4c59ba35e50ee2813c49929feb9a15d5579c7))

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
