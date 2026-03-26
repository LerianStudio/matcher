## [1.3.0-beta.2](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.1...v1.3.0-beta.2) (2026-03-26)


### Features

* **bootstrap:** add alias-aware systemplane manager ([6923821](https://github.com/LerianStudio/matcher/commit/6923821d186c4ead3bcadc9a8d8c727a68c5930b))
* **bootstrap:** add AllowInsecure option to object storage endpoint ([bf3025e](https://github.com/LerianStudio/matcher/commit/bf3025e6b00da27f4b7600ce6dba6147e57c695f))
* **shared/ports:** add IsNilValue nil safety utility ([8f6d52a](https://github.com/LerianStudio/matcher/commit/8f6d52a8f92a57a5e548167c8879f61dec88e4f8))
* **bootstrap:** add logger bundle for structured logging ([ee0dfb6](https://github.com/LerianStudio/matcher/commit/ee0dfb6e6f8bfce3c02e55b9f29204c46f3da6d7))
* **shared/postgres:** add read-only query executor and tx helpers ([3497708](https://github.com/LerianStudio/matcher/commit/34977088332ae9f5c9871ded8cccfe6b86db7ffd))
* **migrations:** add systemplane config key renames migration ([6896b19](https://github.com/LerianStudio/matcher/commit/6896b195f836417bbaa8aeffb3ae20cac6ac5042))
* **bootstrap:** implement systemplane config with legacy key aliases ([d58d6bb](https://github.com/LerianStudio/matcher/commit/d58d6bbfcb37b1040aaa7fda2379d8d827a86625))
* **shared/http:** scope idempotency keys by principal and query string ([e6d7c91](https://github.com/LerianStudio/matcher/commit/e6d7c91d5429a3a9edd83d4dbd39dce8060fada8))


### Bug Fixes

* **bootstrap:** add defensive guards for RabbitMQ lifecycle ([46bde95](https://github.com/LerianStudio/matcher/commit/46bde95cca7d6a3f94b149d794d0ed10174bad14))
* address CodeRabbit review findings across codebase ([59453ec](https://github.com/LerianStudio/matcher/commit/59453ec2637e1bc9d1661a1de26a2466961e92cc))
* **shared/http:** harden idempotency middleware safety ([d06f0f1](https://github.com/LerianStudio/matcher/commit/d06f0f1903a6c4e206cfedc753a65230b1897d78))
* **bootstrap:** hash secret in cache key and tighten insecure env guard ([3750f24](https://github.com/LerianStudio/matcher/commit/3750f245c2e23d2ec0226447e9b9959acf1808df))
* **governance:** improve archival worker resilience ([50c501c](https://github.com/LerianStudio/matcher/commit/50c501c50d5a3af99422a53dab5f507c4be31ea3))
* **bootstrap:** propagate context through logger sync ([cf3e909](https://github.com/LerianStudio/matcher/commit/cf3e90906b6cd7e42a78454df7d80c9956650b39))
* **exception:** reacquire failed idempotency keys and propagate sentinel errors ([0e0e46f](https://github.com/LerianStudio/matcher/commit/0e0e46f9293d204ae459192a5444ce100d91bf30))
* **shared/postgres:** remove redundant nil check and add reverse config map ([cdbb2d9](https://github.com/LerianStudio/matcher/commit/cdbb2d9522c58f028f8b56df5d49bd969e5080c0))
* **shared:** replace deprecated fasthttp Args.VisitAll with All iterator ([9f4c967](https://github.com/LerianStudio/matcher/commit/9f4c967fc7b6faa65d476c599a6a03480bf20637))
* **shared:** skip body-hash idempotency fallback for PATCH requests ([5cbc2f7](https://github.com/LerianStudio/matcher/commit/5cbc2f76d6faea5394008fb156384ec60dd95450))
* **discovery:** suppress false-positive gosec G704 on validated fetcher URL ([3401209](https://github.com/LerianStudio/matcher/commit/34012096399521377cadacc55318bce95a596e11))

## [1.3.0-beta.1](https://github.com/LerianStudio/matcher/compare/v1.2.1...v1.3.0-beta.1) (2026-03-23)


### Features

* **rabbitmq:** add multi-tenant vhost isolation for event publishers ([9a473cb](https://github.com/LerianStudio/matcher/commit/9a473cb1da9184ebaa8183843383f47543cf2d4d))

# Matcher Changelog

## [1.2.1](https://github.com/LerianStudio/matcher/releases/tag/v1.2.1)

- Improvements:
  - Updated CHANGELOGs for matcher to version 1.2.0.
  - Optimized Dockerfile by separating dependency download to leverage caching.

Contributors: @fred, @lerian-studio-midaz-push-bot[bot]

[Compare changes](https://github.com/LerianStudio/matcher/compare/v1.2.0...v1.2.1)

---

## [1.2.1](https://github.com/LerianStudio/matcher/compare/v1.2.0...v1.2.1) (2026-03-23)

# Matcher Changelog

## [1.2.0](https://github.com/LerianStudio/matcher/releases/tag/v1.2.0)

- **Features:**
  - Refactored the ingestion process by splitting the monolithic normalizer into focused modules.
  - Strengthened domain model validation and added mocks in the shared domain.
  - Migrated the bootstrap process to the lib-commons systemplane.

- **Improvements:**
  - Updated the test suite and adapters to align with new APIs.
  - Adapted the service layer to integrate with new shared domain APIs.

Contributors: @fred

[Compare changes](https://github.com/LerianStudio/matcher/compare/v1.1.1...v1.2.0)

---

## [1.2.0](https://github.com/LerianStudio/matcher/compare/v1.1.1...v1.2.0) (2026-03-23)

## [1.1.1](https://github.com/LerianStudio/matcher/compare/v1.1.0...v1.1.1) (2026-03-22)

# Matcher Changelog

## [1.1.0](https://github.com/LerianStudio/matcher/releases/tag/v1.1.0)

- **Features**
  - Introduced fee rules for predicate-based fee calculation.
  - Added fee rule CRUD endpoints and persistence.
  - Implemented predicate value type coercion and size constraints.
  - Added fee rule domain model with predicate-based schedule resolution.
  - Enforced fee rule limits in create/update/delete operations.

- **Fixes**
  - Improved fee rule validation and field predicates.
  - Hardened clone functionality with validation and locks.
  - Added fail-fast guard for missing fee-rule provider.
  - Scoped fee-rule delete by context_id for defense-in-depth.
  - Improved error handling for fee rule validation and migration.

- **Improvements**
  - Split rule execution into focused modules.
  - Decomposed match group commands into focused modules.
  - Replaced source-level fee schedules with rule-based resolution.
  - Validated fee rule predicates at HTTP layer and improved persistence.
  - Enhanced fee rule validation, error handling, and migration.

Contributors: @bedatty, @dependabot[bot], @ferr3ira-gabriel, @ferr3ira.gabriel, @fred, @gandalf, @lucas.bedatty

[Compare changes](https://github.com/LerianStudio/matcher/compare/v1.0.0...v1.1.0)

---

## [1.1.0](https://github.com/LerianStudio/matcher/compare/v1.0.0...v1.1.0) (2026-03-22)


### Features

* **systemplane:** add apply behaviors, store encryption, write validation, and resource lifecycle ([ecfe42a](https://github.com/LerianStudio/matcher/commit/ecfe42a26579cb9e9e4083138dc7ec70c8a15221))
* **systemplane:** add bootstrap config, backend factory registry, and identifier validation ([248f371](https://github.com/LerianStudio/matcher/commit/248f371b86b3c2f8f0538360fd44a3feb04ebc87))
* **systemplane:** add change feed adapters with debounce and resync ([b87e8aa](https://github.com/LerianStudio/matcher/commit/b87e8aaec61ccdf7f4147c744465cf093a01bb7d))
* **systemplane:** add Component field to KeyDef and IncrementalBundleFactory port ([ea3da97](https://github.com/LerianStudio/matcher/commit/ea3da97a5b7f906da4ca105f308a79bab3649aee))
* **fee:** add fee rule count limits and predicate validation constraints ([df2f76b](https://github.com/LerianStudio/matcher/commit/df2f76bbdb7eb7494ec123db92893ecaf13f2881))
* **configuration:** add fee rule CRUD endpoints and persistence ([0f5c8eb](https://github.com/LerianStudio/matcher/commit/0f5c8ebbcfe65344d6df0ada206436b24fa591c2))
* **fee:** add fee rule domain model with predicate-based schedule resolution ([dcd48ea](https://github.com/LerianStudio/matcher/commit/dcd48eae45a5c4050d018a193f5913ff816f7806))
* **configuration:** add fee rule tests and improve swagger docs ([e1d2f59](https://github.com/LerianStudio/matcher/commit/e1d2f5997ceea758025b4f412258389e211875ef))
* **bootstrap:** add fetcher config defaults and validation ([fe3a02b](https://github.com/LerianStudio/matcher/commit/fe3a02b364fe341a2c7c2545d45b0d4b881fde5b))
* **bootstrap:** add fetcher config defaults and validation ([eb6dced](https://github.com/LerianStudio/matcher/commit/eb6dced0309ccb45daec210bc764ab7d83c5f3e0))
* add fetcher integration and discovery bounded context ([5daf5ce](https://github.com/LerianStudio/matcher/commit/5daf5ce1b9f3f1af25ebaa3a021f98e9e1325212))
* **configuration:** add FETCHER source type and regenerate OpenAPI specs ([a96dae8](https://github.com/LerianStudio/matcher/commit/a96dae83133847ca69b1f05e3645949c80d3a7f4))
* **systemplane:** add Fiber HTTP adapter for runtime configuration API ([d4700b1](https://github.com/LerianStudio/matcher/commit/d4700b12d91db58114dabfc1347e88136848cac7))
* **bootstrap:** add incremental build methods to bundle factory ([4063299](https://github.com/LerianStudio/matcher/commit/4063299258d53901d6b7fede8f28f4ae6346ab26))
* **rabbitmq:** add insecure health check policy with multi-layer validation ([cd6f4cb](https://github.com/LerianStudio/matcher/commit/cd6f4cbbefed15777f77d6ac33c2350666df036e))
* **auth:** add LookupTenantID and local JWT claim pre-validation ([b45cf7b](https://github.com/LerianStudio/matcher/commit/b45cf7b09338fd2bbd9fafbf7a490fbf321383ed))
* **configuration:** add matching side to reconciliation sources ([9527e9e](https://github.com/LerianStudio/matcher/commit/9527e9e5d85ac30e26ca930f7540cd90b1600d87))
* **systemplane:** add MongoDB store and history adapters ([8fbca28](https://github.com/LerianStudio/matcher/commit/8fbca2828315e1da685e78ac2349dff0e433e4e1))
* **db:** add official mongodb driver to enable database connectivity ([6ef23ea](https://github.com/LerianStudio/matcher/commit/6ef23ea4884d97122353a3ee1f06179e75039087))
* **systemplane:** add phase 1 runtime configuration core ([dc616ed](https://github.com/LerianStudio/matcher/commit/dc616ed98c93dde6d5bec58dfe9998e5e766c9cc))
* **systemplane:** add PostgreSQL store and history adapters ([5356318](https://github.com/LerianStudio/matcher/commit/5356318d8c29572c39d9e0d164e6be5850d520a8))
* **fee:** add predicate size constraints and require explicit priority ([18cb756](https://github.com/LerianStudio/matcher/commit/18cb7567a86a0c2ceaf49d5c5e675b49d98236d9))
* **systemplane:** add ReconcilerPhase type and phase-sorted reconciler execution ([7e034d9](https://github.com/LerianStudio/matcher/commit/7e034d9c936631e46862ca9156eb211e9d224565))
* **governance:** add runtime storage swapping to archival worker ([64af419](https://github.com/LerianStudio/matcher/commit/64af41921438b506fb96d809e7008c7a074c65a7))
* **systemplane:** add secret codec for at-rest config encryption ([3a9dbc2](https://github.com/LerianStudio/matcher/commit/3a9dbc2cc1d3c56fa9b13a9072c0eb47c622123a))
* **bootstrap:** add swappable handles and dynamic infrastructure adapters ([cefb476](https://github.com/LerianStudio/matcher/commit/cefb476ba8488092593d46fcf4c6f6ea7da30c7b))
* **auth:** add system module action constants for systemplane ([f5ee2f6](https://github.com/LerianStudio/matcher/commit/f5ee2f682276a72046b6085e4e3115d7d1fbe565))
* **swagger:** add systemplane API annotations and regenerate docs ([4a0c70b](https://github.com/LerianStudio/matcher/commit/4a0c70b95fceb9577ab4e56f7efe7b93735d80fc))
* **discovery:** add transactional conditional update for extraction ([c61d200](https://github.com/LerianStudio/matcher/commit/c61d2008750123592a792712a7b1d9bbf52db7a7))
* **workers:** add UpdateRuntimeConfig and safe channel reset for hot-reload ([5a6d14e](https://github.com/LerianStudio/matcher/commit/5a6d14e481dc3dc4aec119afd06caccadf30c638))
* **deps:** add viper for configuration management ([d3c8a06](https://github.com/LerianStudio/matcher/commit/d3c8a06f6a4b7daaba036c1218548ebe7725a799))
* add YAML config support with hot-reload and runtime config API ([7fdc698](https://github.com/LerianStudio/matcher/commit/7fdc69849c249a49880bf0fa01aea6ba388b7053))
* **ci:** changelog ([8ef8d41](https://github.com/LerianStudio/matcher/commit/8ef8d41af905c2789211dbb20c5ffd28b82ecc5a))
* **configuration:** enforce fee rule limits in create/update/delete operations ([d126954](https://github.com/LerianStudio/matcher/commit/d126954c737d0d0042edfa905bdb08f145c06cfe))
* **fee:** enhance fee rule validation, error handling, and migration ([8d40fb2](https://github.com/LerianStudio/matcher/commit/8d40fb227306596aea50f1a4707509a6016a963b))
* **reporting:** expand export report types with EXCEPTIONS and aliases ([d92dab2](https://github.com/LerianStudio/matcher/commit/d92dab2e5457f7c733607f6a715e116bdaed14a8))
* **bootstrap:** expand multi-tenant config with URL, API key, circuit-breaker and pool settings ([43b4256](https://github.com/LerianStudio/matcher/commit/43b4256e280fa380308db121c94f55b04d482df6))
* **configuration:** harden clone functionality with validation and locks ([4bfca34](https://github.com/LerianStudio/matcher/commit/4bfca346d4414a5b45fee1795c9030606189ba32))
* **systemplane:** implement incremental bundle rebuilds in supervisor ([86241be](https://github.com/LerianStudio/matcher/commit/86241bec78b1a997ea89e7a03fed03537e0ecfff))
* **fee:** improve predicate value type coercion ([3253ab8](https://github.com/LerianStudio/matcher/commit/3253ab85c7686280db9b65c469524e8545f0c9fa))
* **config:** introduce fee rules for predicate-based fee calculation ([905a860](https://github.com/LerianStudio/matcher/commit/905a8609a9da1607782f9cca15fc4e16b76f0967))
* **rabbitmq:** propagate X-Tenant-ID header in ingestion and matching event publishers ([7642584](https://github.com/LerianStudio/matcher/commit/76425849a586524d987f376904f4add384076c8a))
* remove SSL/TLS and AUTH_ENABLED production validations ([022cf2b](https://github.com/LerianStudio/matcher/commit/022cf2bf6e98e73f8d0a2849419cb6a2c93a7f21))
* **discovery:** ship extraction workflow and harden discovery sync ([a2784f5](https://github.com/LerianStudio/matcher/commit/a2784f5a3791c424ff5b6933de76b2069f9f3c74))
* **systemplane:** simplify ConfigManager by removing unused versioning and reload tracking ([ae377cf](https://github.com/LerianStudio/matcher/commit/ae377cf559596b5342bb83ae5d7abd5ca1cf42b2))
* **configuration:** support inline source and rule creation in CreateContext ([7038403](https://github.com/LerianStudio/matcher/commit/7038403ccf8a294e36bd67ec256dc7f0aa62342d))
* **configuration:** validate fee rule predicates at HTTP layer and improve persistence ([3c75b5e](https://github.com/LerianStudio/matcher/commit/3c75b5e477acd04fb7df6f26fa58f9579de59d08))
* **systemplane:** wire builtin backend factories for PostgreSQL and MongoDB ([4c1eba7](https://github.com/LerianStudio/matcher/commit/4c1eba7878a1fbbc89b90ea75a3f0f6026650c0a))
* **bootstrap:** wire config manager and worker manager into service lifecycle ([66e1f17](https://github.com/LerianStudio/matcher/commit/66e1f17b79611a1c7561283e94fffb0b3c175108))
* **bootstrap:** wire config manager and worker manager into service lifecycle ([bb21dec](https://github.com/LerianStudio/matcher/commit/bb21dece3d4e841df63a92a29947b3ab9663cbdc))
* **bootstrap:** wire dynamic configGetter, config history API, and service lifecycle ([9e968f5](https://github.com/LerianStudio/matcher/commit/9e968f511f92b4c47b82403d0cd1fe6ce8493884))
* **bootstrap:** wire fee rule repository into configuration use cases ([32e5172](https://github.com/LerianStudio/matcher/commit/32e51724dcdf0c7a987bb5daa9138e61bea79f8d))
* **bootstrap:** wire hot worker config, dynamic rate-limit expiry, and auth-gated config API ([469e3dc](https://github.com/LerianStudio/matcher/commit/469e3dc2c162d836c75746cc5a3deaad6ac51f18))
* **bootstrap:** wire reload observer and pass logger to InitSystemplane ([e57f66d](https://github.com/LerianStudio/matcher/commit/e57f66d2285b235023d0b30f0c6e124042d8411f))
* **bootstrap:** wire systemplane hot reload into bootstrap lifecycle ([2641b0c](https://github.com/LerianStudio/matcher/commit/2641b0c745ab30f3a38b48a6417cac120590d57e))


### Bug Fixes

* **bootstrap:** add configurable health-check timeout and document shutdown ordering ([6ef8732](https://github.com/LerianStudio/matcher/commit/6ef8732785fdafff608419df3d8698a10eeaf306))
* **matching:** add fail-fast guard for missing fee-rule provider ([e5930d2](https://github.com/LerianStudio/matcher/commit/e5930d28b071c8c9fd1da37a422b78f789f97c95))
* **bootstrap:** add HealthCheckTimeoutSec to defaultConfig and bindDefaults ([2e94aa9](https://github.com/LerianStudio/matcher/commit/2e94aa9ec4c0c65873031c9811f062134cf5b6a1))
* **migration:** add idempotent fee normalization column guard ([4f144db](https://github.com/LerianStudio/matcher/commit/4f144db3582687fe789f8ed61b7065feb10a3e2e))
* **governance:** add normalizeArchivalWorkerConfig for defense-in-depth ([a667547](https://github.com/LerianStudio/matcher/commit/a6675475ffba014b69bf2e7912842e67ceb3fbdd))
* **integration:** add RabbitMQ startup retry and remove test parallelism race ([4568b06](https://github.com/LerianStudio/matcher/commit/4568b06f3b15b7cf42649c6e56e5a4e77aed9264))
* **systemplane:** add rollback discard logic and improve error handling ([88ac82f](https://github.com/LerianStudio/matcher/commit/88ac82fc2f9cf9a645a1b889cc49c1e71195dafe))
* **cross:** add typed-nil panic guard, boundary error mapping, and auth empty-action guard ([8555f58](https://github.com/LerianStudio/matcher/commit/8555f58db5dd48d7f26597ded604bde3d5ad8439))
* **bootstrap:** always apply runtime config before worker restart ([5776d12](https://github.com/LerianStudio/matcher/commit/5776d1285e8bb6ce2c08c957e31af0e4a3c5f5d2))
* **bootstrap:** compute field-level diffs in config change tracking ([5684537](https://github.com/LerianStudio/matcher/commit/568453768f2eb647661579878b9e5e1490bc32f0))
* **systemplane:** correct critical runtime orchestration and safety bugs ([7af113b](https://github.com/LerianStudio/matcher/commit/7af113bed81fcf7d3e4f82500f7d29c346bed44f))
* correct test indentation and add nosec annotations ([2336bf8](https://github.com/LerianStudio/matcher/commit/2336bf81dbfa3e9390adc0cd2a07f734bd247699))
* **bootstrap:** defer route registration failure to report all errors ([a119cc1](https://github.com/LerianStudio/matcher/commit/a119cc13bae04e393c65137a1c781d93a9799bf6))
* **bootstrap:** derive SafeError production flag and forward validation errors ([312b9d1](https://github.com/LerianStudio/matcher/commit/312b9d1c522c3acfb960ea07a6bdc6512f0f5859))
* **bootstrap:** enforce rate-limit bounds and remove dead fetcher durations ([a6c1b22](https://github.com/LerianStudio/matcher/commit/a6c1b220a3e73ff12821dd0d03e2f4769cda8cd1))
* **lint:** exclude gosec G118 false-positives for cross-function cancel propagation ([3e714ba](https://github.com/LerianStudio/matcher/commit/3e714ba76b6fbdfc2223d73bd5029758dbb5cb88))
* **bootstrap:** fix typo in fetcher extraction timeout constant name ([4741437](https://github.com/LerianStudio/matcher/commit/47414374727d7f6db0366a8b40553918449d06a0))
* **bootstrap:** group var declarations for style consistency ([b859319](https://github.com/LerianStudio/matcher/commit/b859319e62bdba6f6bc97a2f9108aa96ff3b5cc9))
* **bootstrap:** handle env-overridden updates and idempotent watcher startup ([0872082](https://github.com/LerianStudio/matcher/commit/08720826bf15751737f4913709f1f73b3f0563c1))
* **systemplane:** harden adapters and preserve API contract semantics ([14a7995](https://github.com/LerianStudio/matcher/commit/14a7995fe1d38953228e93e6e0f46c92f2cbd8a3))
* **configuration:** harden clone with FOR SHARE lock and input validation ([5f13a39](https://github.com/LerianStudio/matcher/commit/5f13a39a0ce428cea6da67a1c81931b750f08cdf))
* **bootstrap:** harden config API audit context and env override visibility ([6276538](https://github.com/LerianStudio/matcher/commit/6276538188f544c8726117d206050841a04cedbd))
* **bootstrap:** harden config API auth, tracer fallback, and manager lifecycle ([bcfecc0](https://github.com/LerianStudio/matcher/commit/bcfecc0483f26ddd5ff587bc743ee96d01df17bc))
* **bootstrap:** harden config API, audit, worker factories, and schema ([211b327](https://github.com/LerianStudio/matcher/commit/211b32777d32ff482e01e401b2641eefea28c630))
* **bootstrap:** harden config API, audit, worker factories, and schema ([25e1709](https://github.com/LerianStudio/matcher/commit/25e17093e405dc616eb4271433dfe8915a432f5a))
* **bootstrap:** harden config file path resolution against traversal ([fc4deaa](https://github.com/LerianStudio/matcher/commit/fc4deaa2ed76281fc1c1cd9855d58dfe2ff0f2c8))
* **bootstrap:** harden config manager subscriber lifecycle and source tagging ([f0f5cba](https://github.com/LerianStudio/matcher/commit/f0f5cbac100df2d24fd0889284fe2b4930d66704))
* **bootstrap:** harden config manager with nil guards and type validation ([2751348](https://github.com/LerianStudio/matcher/commit/275134899205b6dc85fc1627fcc8a4b484601f97))
* **bootstrap:** harden config reload lifecycle and nil guards ([aa76163](https://github.com/LerianStudio/matcher/commit/aa761631166f91972ceceaa1ac88a3cf523cad97))
* **discovery:** harden fetcher lifecycle and migration safety ([79f9662](https://github.com/LerianStudio/matcher/commit/79f9662746c8879d83ff7bbbccbb2a687010b8c0))
* **discovery:** harden fetcher lifecycle and startup safety ([7db31bc](https://github.com/LerianStudio/matcher/commit/7db31bc3d9fff369ade400542e7dd522217cf313))
* **bootstrap:** harden runtime config lifecycle ([0efe6b1](https://github.com/LerianStudio/matcher/commit/0efe6b1e24375ef82af2044d9755e2da9fec2ee2))
* **bootstrap:** harden runtime config lifecycle ([f2d01d8](https://github.com/LerianStudio/matcher/commit/f2d01d8c7b8389c5294cb83acdf922052e6df748))
* **bootstrap:** harden runtime config lifecycle ([bbd35b7](https://github.com/LerianStudio/matcher/commit/bbd35b70d1c312183c9d8e773823d60659de5ea7))
* **bootstrap:** harden runtime config lifecycle ([c10c671](https://github.com/LerianStudio/matcher/commit/c10c67131ec28cee622b2392bfb32139483c4036))
* **bootstrap:** harden runtime config updates ([b5dca5b](https://github.com/LerianStudio/matcher/commit/b5dca5bc6addaaf40574a472bbb75dfb9e3cfefc))
* **bootstrap:** harden runtime config updates ([f089042](https://github.com/LerianStudio/matcher/commit/f089042b145771066bda539b1eef4004b2b9a646))
* **bootstrap:** harden subscriber lifecycle, path validation, and atomic write ([1f356ec](https://github.com/LerianStudio/matcher/commit/1f356ece2e41a5b02a09512fe8983110ada25fff))
* **cross:** implement cursor pagination for match rules and sources ([8819878](https://github.com/LerianStudio/matcher/commit/88198784e3b066619b4bd541ea18c1af3eca0a3e))
* **configuration:** improve fee rule validation and field predicates ([c37306d](https://github.com/LerianStudio/matcher/commit/c37306d33b103425867a4eabbabba064da63dbbf))
* **makefile:** isolate unit tests from host environment variables ([3c57128](https://github.com/LerianStudio/matcher/commit/3c571288ee75e084c2dcea4f4cd352ffde5e25fc))
* **bootstrap:** make worker restarts rollback-safe and dynamic-aware ([a3f716f](https://github.com/LerianStudio/matcher/commit/a3f716f2f246bbf9274b2cfad1cd1a081b85574b))
* **bootstrap:** preserve env-explicit zero overrides and redact URI credentials ([7874c6e](https://github.com/LerianStudio/matcher/commit/7874c6ecbc1655ab7b2ea44977823ac033f66b95))
* **bootstrap:** redact sensitive values in config audit change maps ([f66d07f](https://github.com/LerianStudio/matcher/commit/f66d07f21885c5682e023cd9be09dec4c0e6fa49))
* regenerate broken mocks for type-aliased interfaces and correct nosec directive ([e261521](https://github.com/LerianStudio/matcher/commit/e2615212eb1c8322ead879a364e0ebc9395ddcd3))
* **exception:** register bulk routes before parameterized :exceptionId routes ([7c84ff4](https://github.com/LerianStudio/matcher/commit/7c84ff4be0dd332ebf1507a8e03365cd9165bc31))
* **bootstrap:** remove stale mutable config keys and align test signature ([2e428ea](https://github.com/LerianStudio/matcher/commit/2e428ea8acd6846132fbda4eb020be128b3af999))
* **e2e:** resolve dashboard stresser failures and enable archival routes ([b4f6d5e](https://github.com/LerianStudio/matcher/commit/b4f6d5ef73cea658dc230ba7a4835973ffa3d094))
* **e2e:** resolve dashboard stresser flakiness with unique names ([7e93cfc](https://github.com/LerianStudio/matcher/commit/7e93cfcc13d9d074eceebe09b39e9f8b7f7f42ed))
* resolve data race on productionMode and remove stale nolint:gosec directives ([6045dd1](https://github.com/LerianStudio/matcher/commit/6045dd16d36aeec5e664ee13bf60ac53c388debd))
* **bootstrap:** resolve runtime config merge conflicts ([80564f7](https://github.com/LerianStudio/matcher/commit/80564f7e9b6cdae80b0403cbb63de4fe252f0222))
* **swagger:** restore API error schemas for generated docs ([a991db9](https://github.com/LerianStudio/matcher/commit/a991db943d4762b6dc0b400313aa658e33db7112))
* **bootstrap:** restore tenant isolation and harden runtime lifecycle ([cec542e](https://github.com/LerianStudio/matcher/commit/cec542e24c3df52d7089314898aef526919f4c55))
* **lint:** revert nosec G107 to G704 and scope G118 exclusions to specific files ([b79833d](https://github.com/LerianStudio/matcher/commit/b79833dc8c236c1efb8b9bfb6951ec2843136012))
* run go mod vendor in Dockerfile before build ([24253d1](https://github.com/LerianStudio/matcher/commit/24253d12322c908b5733fa343032dbdaab9babd0))
* **configuration:** scope fee-rule delete by context_id for defense-in-depth ([30ccd9b](https://github.com/LerianStudio/matcher/commit/30ccd9b7a28b8a61c123a2af7fc0b57f5a383088))
* **bootstrap:** set proxy header when trusted proxies are configured ([dcfcf0c](https://github.com/LerianStudio/matcher/commit/dcfcf0c61a2592be2e4f7e9916a39957a0d9747a))
* **bootstrap:** tighten runtime worker orchestration ([729d1a2](https://github.com/LerianStudio/matcher/commit/729d1a2d2a6ed820bdbf920f0e037416cf02f076))
* **bootstrap:** tighten runtime worker orchestration ([78f4c59](https://github.com/LerianStudio/matcher/commit/78f4c59ba35e50ee2813c49929feb9a15d5579c7))
* treat permission denied as graceful fallback in config file loading ([007c00c](https://github.com/LerianStudio/matcher/commit/007c00ca04cc0e9645f934b08e710a6fe957344c)), closes [#52](https://github.com/LerianStudio/matcher/issues/52)
* **governance:** truncate test timestamps to microsecond precision for sqlmock ([14b8cd1](https://github.com/LerianStudio/matcher/commit/14b8cd19e6db45a6b0504ceb81f13d9a7621a4f7))
* **systemplane:** update DTOs and service helpers for API contract compliance ([d25a66a](https://github.com/LerianStudio/matcher/commit/d25a66a75d519dd68c8155921d4168a91c045b48))
* **ci:** update gitops workflow parameters to match shared workflow v1.15.0 ([66ed158](https://github.com/LerianStudio/matcher/commit/66ed1584b52d5a4e98395f3a0af1328bf632d861))
* update gofiber to v2.52.12 (CVE-2026-25882) ([c475bab](https://github.com/LerianStudio/matcher/commit/c475bab41e28389cc9dc01a44515bdce319275bf))
* use embedded migrations to eliminate filesystem access ([d25627e](https://github.com/LerianStudio/matcher/commit/d25627e87080d4f42387291a2d19c44f27188dd5))
* **systemplane:** use persisted kind for secret history decryption ([df1b46f](https://github.com/LerianStudio/matcher/commit/df1b46f3fce1127e58c4f085e0c2f0c66c1ea09a))


### Performance Improvements

* **air:** enable exclude_unchanged to improve live-reload performance ([d612be7](https://github.com/LerianStudio/matcher/commit/d612be7675e79281d05acbdfc681555e6df680bd))

## [1.1.0-beta.24](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.23...v1.1.0-beta.24) (2026-03-22)

## [1.1.0-beta.23](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.22...v1.1.0-beta.23) (2026-03-22)

## [1.1.0-beta.22](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.21...v1.1.0-beta.22) (2026-03-22)


### Bug Fixes

* **bootstrap:** defer route registration failure to report all errors ([a119cc1](https://github.com/LerianStudio/matcher/commit/a119cc13bae04e393c65137a1c781d93a9799bf6))

## [1.1.0-beta.21](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.20...v1.1.0-beta.21) (2026-03-22)


### Features

* **fee:** add fee rule count limits and predicate validation constraints ([df2f76b](https://github.com/LerianStudio/matcher/commit/df2f76bbdb7eb7494ec123db92893ecaf13f2881))
* **configuration:** add fee rule CRUD endpoints and persistence ([0f5c8eb](https://github.com/LerianStudio/matcher/commit/0f5c8ebbcfe65344d6df0ada206436b24fa591c2))
* **fee:** add fee rule domain model with predicate-based schedule resolution ([dcd48ea](https://github.com/LerianStudio/matcher/commit/dcd48eae45a5c4050d018a193f5913ff816f7806))
* **configuration:** add fee rule tests and improve swagger docs ([e1d2f59](https://github.com/LerianStudio/matcher/commit/e1d2f5997ceea758025b4f412258389e211875ef))
* **configuration:** add matching side to reconciliation sources ([9527e9e](https://github.com/LerianStudio/matcher/commit/9527e9e5d85ac30e26ca930f7540cd90b1600d87))
* **fee:** add predicate size constraints and require explicit priority ([18cb756](https://github.com/LerianStudio/matcher/commit/18cb7567a86a0c2ceaf49d5c5e675b49d98236d9))
* **configuration:** enforce fee rule limits in create/update/delete operations ([d126954](https://github.com/LerianStudio/matcher/commit/d126954c737d0d0042edfa905bdb08f145c06cfe))
* **fee:** enhance fee rule validation, error handling, and migration ([8d40fb2](https://github.com/LerianStudio/matcher/commit/8d40fb227306596aea50f1a4707509a6016a963b))
* **configuration:** harden clone functionality with validation and locks ([4bfca34](https://github.com/LerianStudio/matcher/commit/4bfca346d4414a5b45fee1795c9030606189ba32))
* **fee:** improve predicate value type coercion ([3253ab8](https://github.com/LerianStudio/matcher/commit/3253ab85c7686280db9b65c469524e8545f0c9fa))
* **config:** introduce fee rules for predicate-based fee calculation ([905a860](https://github.com/LerianStudio/matcher/commit/905a8609a9da1607782f9cca15fc4e16b76f0967))
* **configuration:** validate fee rule predicates at HTTP layer and improve persistence ([3c75b5e](https://github.com/LerianStudio/matcher/commit/3c75b5e477acd04fb7df6f26fa58f9579de59d08))
* **bootstrap:** wire fee rule repository into configuration use cases ([32e5172](https://github.com/LerianStudio/matcher/commit/32e51724dcdf0c7a987bb5daa9138e61bea79f8d))


### Bug Fixes

* **matching:** add fail-fast guard for missing fee-rule provider ([e5930d2](https://github.com/LerianStudio/matcher/commit/e5930d28b071c8c9fd1da37a422b78f789f97c95))
* **cross:** add typed-nil panic guard, boundary error mapping, and auth empty-action guard ([8555f58](https://github.com/LerianStudio/matcher/commit/8555f58db5dd48d7f26597ded604bde3d5ad8439))
* **configuration:** harden clone with FOR SHARE lock and input validation ([5f13a39](https://github.com/LerianStudio/matcher/commit/5f13a39a0ce428cea6da67a1c81931b750f08cdf))
* **cross:** implement cursor pagination for match rules and sources ([8819878](https://github.com/LerianStudio/matcher/commit/88198784e3b066619b4bd541ea18c1af3eca0a3e))
* **configuration:** improve fee rule validation and field predicates ([c37306d](https://github.com/LerianStudio/matcher/commit/c37306d33b103425867a4eabbabba064da63dbbf))
* **configuration:** scope fee-rule delete by context_id for defense-in-depth ([30ccd9b](https://github.com/LerianStudio/matcher/commit/30ccd9b7a28b8a61c123a2af7fc0b57f5a383088))

## [1.1.0-beta.20](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.19...v1.1.0-beta.20) (2026-03-21)


### Features

* **systemplane:** add apply behaviors, store encryption, write validation, and resource lifecycle ([ecfe42a](https://github.com/LerianStudio/matcher/commit/ecfe42a26579cb9e9e4083138dc7ec70c8a15221))
* **systemplane:** add bootstrap config, backend factory registry, and identifier validation ([248f371](https://github.com/LerianStudio/matcher/commit/248f371b86b3c2f8f0538360fd44a3feb04ebc87))
* **systemplane:** add change feed adapters with debounce and resync ([b87e8aa](https://github.com/LerianStudio/matcher/commit/b87e8aaec61ccdf7f4147c744465cf093a01bb7d))
* **systemplane:** add Component field to KeyDef and IncrementalBundleFactory port ([ea3da97](https://github.com/LerianStudio/matcher/commit/ea3da97a5b7f906da4ca105f308a79bab3649aee))
* **systemplane:** add Fiber HTTP adapter for runtime configuration API ([d4700b1](https://github.com/LerianStudio/matcher/commit/d4700b12d91db58114dabfc1347e88136848cac7))
* **bootstrap:** add incremental build methods to bundle factory ([4063299](https://github.com/LerianStudio/matcher/commit/4063299258d53901d6b7fede8f28f4ae6346ab26))
* **systemplane:** add MongoDB store and history adapters ([8fbca28](https://github.com/LerianStudio/matcher/commit/8fbca2828315e1da685e78ac2349dff0e433e4e1))
* **db:** add official mongodb driver to enable database connectivity ([6ef23ea](https://github.com/LerianStudio/matcher/commit/6ef23ea4884d97122353a3ee1f06179e75039087))
* **systemplane:** add phase 1 runtime configuration core ([dc616ed](https://github.com/LerianStudio/matcher/commit/dc616ed98c93dde6d5bec58dfe9998e5e766c9cc))
* **systemplane:** add PostgreSQL store and history adapters ([5356318](https://github.com/LerianStudio/matcher/commit/5356318d8c29572c39d9e0d164e6be5850d520a8))
* **systemplane:** add ReconcilerPhase type and phase-sorted reconciler execution ([7e034d9](https://github.com/LerianStudio/matcher/commit/7e034d9c936631e46862ca9156eb211e9d224565))
* **governance:** add runtime storage swapping to archival worker ([64af419](https://github.com/LerianStudio/matcher/commit/64af41921438b506fb96d809e7008c7a074c65a7))
* **systemplane:** add secret codec for at-rest config encryption ([3a9dbc2](https://github.com/LerianStudio/matcher/commit/3a9dbc2cc1d3c56fa9b13a9072c0eb47c622123a))
* **bootstrap:** add swappable handles and dynamic infrastructure adapters ([cefb476](https://github.com/LerianStudio/matcher/commit/cefb476ba8488092593d46fcf4c6f6ea7da30c7b))
* **auth:** add system module action constants for systemplane ([f5ee2f6](https://github.com/LerianStudio/matcher/commit/f5ee2f682276a72046b6085e4e3115d7d1fbe565))
* **swagger:** add systemplane API annotations and regenerate docs ([4a0c70b](https://github.com/LerianStudio/matcher/commit/4a0c70b95fceb9577ab4e56f7efe7b93735d80fc))
* **systemplane:** implement incremental bundle rebuilds in supervisor ([86241be](https://github.com/LerianStudio/matcher/commit/86241bec78b1a997ea89e7a03fed03537e0ecfff))
* **systemplane:** simplify ConfigManager by removing unused versioning and reload tracking ([ae377cf](https://github.com/LerianStudio/matcher/commit/ae377cf559596b5342bb83ae5d7abd5ca1cf42b2))
* **systemplane:** wire builtin backend factories for PostgreSQL and MongoDB ([4c1eba7](https://github.com/LerianStudio/matcher/commit/4c1eba7878a1fbbc89b90ea75a3f0f6026650c0a))
* **bootstrap:** wire reload observer and pass logger to InitSystemplane ([e57f66d](https://github.com/LerianStudio/matcher/commit/e57f66d2285b235023d0b30f0c6e124042d8411f))
* **bootstrap:** wire systemplane hot reload into bootstrap lifecycle ([2641b0c](https://github.com/LerianStudio/matcher/commit/2641b0c745ab30f3a38b48a6417cac120590d57e))


### Bug Fixes

* **systemplane:** add rollback discard logic and improve error handling ([88ac82f](https://github.com/LerianStudio/matcher/commit/88ac82fc2f9cf9a645a1b889cc49c1e71195dafe))
* **systemplane:** correct critical runtime orchestration and safety bugs ([7af113b](https://github.com/LerianStudio/matcher/commit/7af113bed81fcf7d3e4f82500f7d29c346bed44f))
* **systemplane:** harden adapters and preserve API contract semantics ([14a7995](https://github.com/LerianStudio/matcher/commit/14a7995fe1d38953228e93e6e0f46c92f2cbd8a3))
* **exception:** register bulk routes before parameterized :exceptionId routes ([7c84ff4](https://github.com/LerianStudio/matcher/commit/7c84ff4be0dd332ebf1507a8e03365cd9165bc31))
* **e2e:** resolve dashboard stresser failures and enable archival routes ([b4f6d5e](https://github.com/LerianStudio/matcher/commit/b4f6d5ef73cea658dc230ba7a4835973ffa3d094))
* **bootstrap:** restore tenant isolation and harden runtime lifecycle ([cec542e](https://github.com/LerianStudio/matcher/commit/cec542e24c3df52d7089314898aef526919f4c55))
* **systemplane:** update DTOs and service helpers for API contract compliance ([d25a66a](https://github.com/LerianStudio/matcher/commit/d25a66a75d519dd68c8155921d4168a91c045b48))
* **systemplane:** use persisted kind for secret history decryption ([df1b46f](https://github.com/LerianStudio/matcher/commit/df1b46f3fce1127e58c4f085e0c2f0c66c1ea09a))

## [1.1.0-beta.19](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.18...v1.1.0-beta.19) (2026-03-21)


### Bug Fixes

* treat permission denied as graceful fallback in config file loading ([007c00c](https://github.com/LerianStudio/matcher/commit/007c00ca04cc0e9645f934b08e710a6fe957344c)), closes [#52](https://github.com/LerianStudio/matcher/issues/52)

## [1.1.0-beta.18](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.17...v1.1.0-beta.18) (2026-03-21)

## [1.1.0-beta.17](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.16...v1.1.0-beta.17) (2026-03-21)

## [1.1.0-beta.16](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.15...v1.1.0-beta.16) (2026-03-21)

## [1.1.0-beta.15](https://github.com/LerianStudio/matcher/compare/v1.1.0-beta.14...v1.1.0-beta.15) (2026-03-14)


### Features

* **auth:** add LookupTenantID and local JWT claim pre-validation ([b45cf7b](https://github.com/LerianStudio/matcher/commit/b45cf7b09338fd2bbd9fafbf7a490fbf321383ed))
* **bootstrap:** expand multi-tenant config with URL, API key, circuit-breaker and pool settings ([43b4256](https://github.com/LerianStudio/matcher/commit/43b4256e280fa380308db121c94f55b04d482df6))
* **rabbitmq:** propagate X-Tenant-ID header in ingestion and matching event publishers ([7642584](https://github.com/LerianStudio/matcher/commit/76425849a586524d987f376904f4add384076c8a))

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
