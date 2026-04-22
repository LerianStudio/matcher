## [1.4.0-beta.4](https://github.com/LerianStudio/matcher/compare/v1.4.0-beta.3...v1.4.0-beta.4) (2026-04-22)


### Bug Fixes

* **tests:** resolve make ci gaps from simplify wave ([bd00bed](https://github.com/LerianStudio/matcher/commit/bd00bed66ab73f2a7b8a7d4bd4c32946cfb5e85a))

## [1.4.0-beta.3](https://github.com/LerianStudio/matcher/compare/v1.4.0-beta.2...v1.4.0-beta.3) (2026-04-22)

## [1.4.0-beta.2](https://github.com/LerianStudio/matcher/compare/v1.4.0-beta.1...v1.4.0-beta.2) (2026-04-20)


### Features

* **bootstrap:** production-grade /readyz endpoint with startup probe and TLS posture ([c9a830b](https://github.com/LerianStudio/matcher/commit/c9a830bb0ae2a6ab378a5e4ae6d5124f59bb728d))

## [1.4.0-beta.1](https://github.com/LerianStudio/matcher/compare/v1.3.0...v1.4.0-beta.1) (2026-04-20)

## [1.3.0](https://github.com/LerianStudio/matcher/compare/v1.2.1...v1.3.0) (2026-04-20)


### Features

* **bootstrap:** add alias-aware systemplane manager ([6923821](https://github.com/LerianStudio/matcher/commit/6923821d186c4ead3bcadc9a8d8c727a68c5930b))
* **bootstrap:** add AllowInsecure option to object storage endpoint ([bf3025e](https://github.com/LerianStudio/matcher/commit/bf3025e6b00da27f4b7600ce6dba6147e57c695f))
* **auth:** add Casdoor seed generator ([3877651](https://github.com/LerianStudio/matcher/commit/3877651c6563f136cbb771386e1845d55be630ef))
* **bootstrap:** add dedicated admin rate-limit tier for /system plane ([2f65348](https://github.com/LerianStudio/matcher/commit/2f653480c13922f9b009be74c619feb4c3be2e76))
* **shared/ports:** add IsNilValue nil safety utility ([8f6d52a](https://github.com/LerianStudio/matcher/commit/8f6d52a8f92a57a5e548167c8879f61dec88e4f8))
* **bootstrap:** add logger bundle for structured logging ([ee0dfb6](https://github.com/LerianStudio/matcher/commit/ee0dfb6e6f8bfce3c02e55b9f29204c46f3da6d7))
* **m2m:** add M2M credential provider with AWS Secrets Manager ([d658c86](https://github.com/LerianStudio/matcher/commit/d658c860016b61c599ed0dff4dfe323abc70582e))
* **bootstrap:** add migration preflight guards ([59b43b2](https://github.com/LerianStudio/matcher/commit/59b43b297e3cabfcbba0efc34fe5d3c9fdd0e0dc))
* **rabbitmq:** add multi-tenant vhost isolation for event publishers ([9a473cb](https://github.com/LerianStudio/matcher/commit/9a473cb1da9184ebaa8183843383f47543cf2d4d))
* **shared/postgres:** add read-only query executor and tx helpers ([3497708](https://github.com/LerianStudio/matcher/commit/34977088332ae9f5c9871ded8cccfe6b86db7ffd))
* **bootstrap:** add runtime settings layer ([fa73f05](https://github.com/LerianStudio/matcher/commit/fa73f059b226a50ffd4e267ad5da17ea2f7aebf0))
* **migrations:** add systemplane config key renames migration ([6896b19](https://github.com/LerianStudio/matcher/commit/6896b195f836417bbaa8aeffb3ae20cac6ac5042))
* **fetcher-bridge:** add trusted stream intake boundary (T-001) ([7a74051](https://github.com/LerianStudio/matcher/commit/7a74051dea2735500aaad6b53495e0576c8ebbe2))
* **bootstrap:** apply global rate limit to systemplane admin API ([773b331](https://github.com/LerianStudio/matcher/commit/773b331bb0fd70bf6266784a58906219ea73489a))
* **fetcher-bridge:** automatic completed-extraction bridging worker (T-003) ([efa0bbe](https://github.com/LerianStudio/matcher/commit/efa0bbeef578390561db8522164846c9122ca4a4))
* **multi-tenant:** cache tenant middleware with TenantCache and RWMutex ([cac4412](https://github.com/LerianStudio/matcher/commit/cac4412de5c44c768a7e3c3320872892454083f2))
* **reporting:** cursor-based pagination for export job listings ([0174364](https://github.com/LerianStudio/matcher/commit/0174364a545337710210837c5a07fd17efb4b563))
* **exception:** double actor-hash width and introduce SaltProvider hook ([1e7ea09](https://github.com/LerianStudio/matcher/commit/1e7ea096ba67ba718108a246b3882735a5d335d6))
* **systemplane:** drop orphan bootstrap-only rows and warn on future drift ([b4eeb91](https://github.com/LerianStudio/matcher/commit/b4eeb91f5ac68b2e20e8a010385c89026fbdd65c))
* **outbox:** enforce 1MiB payload cap and truncate oversized audit diffs ([d9dd847](https://github.com/LerianStudio/matcher/commit/d9dd847f55ca427ce5f6fc9413c0c07ca53def1a))
* **multi-tenant:** expand tenancy config with Redis, caching, timeout, and M2M settings ([4c02f5e](https://github.com/LerianStudio/matcher/commit/4c02f5e0f074059aebc583690cb782e26ac38f5a))
* **outbox:** expose typed status helpers from shared domain ([897931b](https://github.com/LerianStudio/matcher/commit/897931b57e8d28a817c44cd434d923e7fceccc8e))
* **bridge-worker:** fan tenants out up to BridgeTenantConcurrency per cycle ([c42df0b](https://github.com/LerianStudio/matcher/commit/c42df0bcce90b2f571eb715dc506dfb07e934d9a))
* **bootstrap:** guard runtime CORS and body-limit edits via systemplane validators ([ecbb65a](https://github.com/LerianStudio/matcher/commit/ecbb65aae3c77dc117b9be3adf5785f4a39316c2))
* **bootstrap:** implement systemplane config with legacy key aliases ([d58d6bb](https://github.com/LerianStudio/matcher/commit/d58d6bbfcb37b1040aaa7fda2379d8d827a86625))
* **exception:** improve dispatch error handling and migrate external_system to varchar ([fe15a8a](https://github.com/LerianStudio/matcher/commit/fe15a8af0cf788c963dda965de513a2f158e4d1d))
* **discovery:** integrate M2M credentials into Fetcher client ([623a01d](https://github.com/LerianStudio/matcher/commit/623a01df2b4f21e52c2c21c18dbb318dd8041322))
* **systemplane:** migrate admin API and config manager to lib-commons v5 ([dd9bcbf](https://github.com/LerianStudio/matcher/commit/dd9bcbf54fb9b20e0e9c952f55afb42e55c02039))
* **bootstrap:** reclassify auth, default-tenant, and outbox keys as bootstrap-only ([00f05a1](https://github.com/LerianStudio/matcher/commit/00f05a12b54fe1b12d06284f5ea3ce3bdac7095b))
* **fetcher-bridge:** retention operations with converging sweep (T-006) ([b148cee](https://github.com/LerianStudio/matcher/commit/b148cee9ad30fd626b7e66f07d4a20f41f5ee130))
* **fetcher-bridge:** retry-safe failure and staleness control (T-005) ([15daeb8](https://github.com/LerianStudio/matcher/commit/15daeb8068c5bba985b22f38fae0cdbdbe859d6d))
* **governance:** return persisted entity from actor mapping upsert ([294d3c5](https://github.com/LerianStudio/matcher/commit/294d3c5091a9a22336b2bc3678799d4a93aad245))
* **discovery:** rewrite Fetcher integration with OAuth2 and schema qualification ([6cffce2](https://github.com/LerianStudio/matcher/commit/6cffce20deb9b79b517942445bba9c7165d04006))
* **shared/http:** scope idempotency keys by principal and query string ([e6d7c91](https://github.com/LerianStudio/matcher/commit/e6d7c91d5429a3a9edd83d4dbd39dce8060fada8))
* **outbox:** shared truncation helpers, single-marshal redesign, match-event ID guard ([0022fd5](https://github.com/LerianStudio/matcher/commit/0022fd53a3416d7157eac20dd439959b3641c788))
* **bootstrap:** shim removed v4 admin paths with 410 Gone ([a48cb64](https://github.com/LerianStudio/matcher/commit/a48cb647048e91dc991146ebc9a475a87285d34a))
* **auth:** split actor-mapping deanonymization into its own RBAC action ([179743d](https://github.com/LerianStudio/matcher/commit/179743db91cc3f9ae280cdf37e5a81bbff9cf52c))
* **governance:** surface audit truncation markers as first-class DTO fields ([09cb6ce](https://github.com/LerianStudio/matcher/commit/09cb6ce187fe54b41233fc921ce2fbd6d7101866))
* **migrations:** tighten bridge eligibility index and reindex concurrently ([51bb683](https://github.com/LerianStudio/matcher/commit/51bb6831a9ec65d2fb844359889371b7ffcf1986))
* **fetcher-bridge:** truthful operational readiness projection (T-004) ([f0f32ca](https://github.com/LerianStudio/matcher/commit/f0f32ca81a695469cbdaea35876cd6ddb967bc2c))
* **fetcher-bridge:** verified artifact retrieval and custody (T-002) ([6e2dad0](https://github.com/LerianStudio/matcher/commit/6e2dad0f0356a7b4cb04debb212f19d827a76fc3))
* **bootstrap:** warn on missing GOMEMLIMIT in cgroup-capped containers ([ef0cbda](https://github.com/LerianStudio/matcher/commit/ef0cbda0234feac0a986bd113aa6140a4a4944dd))


### Bug Fixes

* **bootstrap:** add defensive guards for RabbitMQ lifecycle ([46bde95](https://github.com/LerianStudio/matcher/commit/46bde95cca7d6a3f94b149d794d0ed10174bad14))
* **shared:** address CI lint and CodeRabbit review findings ([f1b3cb4](https://github.com/LerianStudio/matcher/commit/f1b3cb400a7b8e3fad0d8c1d66a71045a46379ce))
* address CodeRabbit review - clarify lib-commons refs, align Go version, fix Swagger path ([ce8e023](https://github.com/LerianStudio/matcher/commit/ce8e023ac0c75d081c7c82bfba9f04d932c54d92))
* address CodeRabbit review - remove stale toolchain ref, add Swagger path ([12ed48c](https://github.com/LerianStudio/matcher/commit/12ed48c4910dc8ed0d9748afe9c33ca802ed2515))
* address CodeRabbit review feedback on lib-commons v5 migration ([e5408a2](https://github.com/LerianStudio/matcher/commit/e5408a2258f4a682000a243048a6e6056962bdfd))
* address CodeRabbit review findings across codebase ([59453ec](https://github.com/LerianStudio/matcher/commit/59453ec2637e1bc9d1661a1de26a2466961e92cc))
* address second round of CodeRabbit review feedback ([d27a6e3](https://github.com/LerianStudio/matcher/commit/d27a6e325521f45acdb65ff0a633e6b23ad3cfcb)), closes [#106](https://github.com/LerianStudio/matcher/issues/106)
* **discovery:** align Fetcher client paths with actual service routes ([e11c23e](https://github.com/LerianStudio/matcher/commit/e11c23e6fd76937ca8b01bfb19cb6d59c34c3640))
* **dashboard:** align MatchRate to percentage scale across repo ([b851ad2](https://github.com/LerianStudio/matcher/commit/b851ad26537c53870de64753250f4c6a9cc3455b))
* apply CodeRabbit review fixes ([ac337ac](https://github.com/LerianStudio/matcher/commit/ac337acac89168acd8f3e860310221023bdfa382))
* apply CodeRabbit review fixes (round 2) ([9c44cf7](https://github.com/LerianStudio/matcher/commit/9c44cf7620b0c14cd27fc66b03c2f65f4836f1e0))
* apply CodeRabbit review fixes (round 3) ([ef8c4c7](https://github.com/LerianStudio/matcher/commit/ef8c4c7a29cfb4eefaea2ffbb0bbad43b6afea17))
* **bootstrap:** apply typed-nil guard to ingestion and match publish helpers ([d068ab7](https://github.com/LerianStudio/matcher/commit/d068ab743ae970c3bd81b9e1c27a04b0704ba526))
* **test-harness:** avoid import cycle in outbox helper default-tenant wiring ([54a2ccf](https://github.com/LerianStudio/matcher/commit/54a2ccf9905a6fad5933f7c5c75b85200d81d15a))
* **security:** bind mock fetcher server to 127.0.0.1 ([a1264ed](https://github.com/LerianStudio/matcher/commit/a1264ed9ff96e351a2c0ba6917b1abd610bcf274))
* **security:** broaden SSRF deny-list with unspecified, multicast, and CGNAT ([2651d62](https://github.com/LerianStudio/matcher/commit/2651d62936bae6272e9182c37168be085aa0ab80))
* **docker:** bump Go builder to 1.26.2-alpine for stdlib CVE patches ([03e35ef](https://github.com/LerianStudio/matcher/commit/03e35efb770c229f7fb259a1d4298f78c8284d58))
* **bootstrap:** classify outbox payload cap and JSON errors as non-retryable ([48a376c](https://github.com/LerianStudio/matcher/commit/48a376ca03efa744a8897156fc1528e862d05035))
* **migrations:** drop CONCURRENTLY on partitioned audit_logs index ([d65e82d](https://github.com/LerianStudio/matcher/commit/d65e82de28393ed3400c9f007052aac3e6ce325a))
* **test:** eliminate race in ExactlyAtCap outbox payload test ([77ea37f](https://github.com/LerianStudio/matcher/commit/77ea37fc9816e5fb2766a7745bf1e117e0a04603))
* **security:** enforce minimum interval between reconciliation schedule firings ([66682b1](https://github.com/LerianStudio/matcher/commit/66682b1dcf43da2b548dae1066075fbbcc33409f))
* **discovery:** enforce uniqueness of Fetcher connection_id per source ([956ff49](https://github.com/LerianStudio/matcher/commit/956ff49531c653167648f93a0a32c4bd717a5a05))
* **tests:** extract rate-limit override into subpackage to break import cycle ([cfa0e0e](https://github.com/LerianStudio/matcher/commit/cfa0e0ef7ee8fa08852f396a9e67f8d1cf740f42))
* **security:** fail bootstrap when systemplane admin API mount fails ([d8d2d50](https://github.com/LerianStudio/matcher/commit/d8d2d5091308689e72604277635d66c8ec173409))
* **security:** fail closed when CreateAdjustment is missing authenticated user ([e9effc4](https://github.com/LerianStudio/matcher/commit/e9effc4f2886f7f837e79513ef4310462514f67a))
* **bootstrap:** flatten config patch apply flow ([af6e886](https://github.com/LerianStudio/matcher/commit/af6e88684d14e211d632b048245fb9bc55d84c05))
* **bootstrap:** guard defaultTenantDiscoverer against nil receiver and inner ([83a6150](https://github.com/LerianStudio/matcher/commit/83a6150537419411cfc7c96cf68e76491cdaa807))
* **test-harness:** guard nil dereferences in integration harnesses ([b367a88](https://github.com/LerianStudio/matcher/commit/b367a880493edf5242483f2a554f1653be6b531a))
* **nil-safety:** guard nil receivers and dependencies in bootstrap + exception gateway ([961b199](https://github.com/LerianStudio/matcher/commit/961b19957514a539ab7bcb1e2387127b8b6e2ee7))
* **nil-safety:** guard worker loops and publishers against nil elements ([019b809](https://github.com/LerianStudio/matcher/commit/019b8091f1c0bcf1565a4d4a31061fde4338a636))
* **shared/http:** harden idempotency middleware safety ([d06f0f1](https://github.com/LerianStudio/matcher/commit/d06f0f1903a6c4e206cfedc753a65230b1897d78))
* **scheduler:** harden SchedulerWorker.Stop against concurrent callers ([fae20db](https://github.com/LerianStudio/matcher/commit/fae20db02787c26bdbda75033de4d43f553aeec5))
* **reporting:** harden streaming iterators against nil rows ([dadeb19](https://github.com/LerianStudio/matcher/commit/dadeb19e01b35f9fe277d3139bb5dfa61aeca77a))
* **bootstrap:** hash secret in cache key and tighten insecure env guard ([3750f24](https://github.com/LerianStudio/matcher/commit/3750f245c2e23d2ec0226447e9b9959acf1808df))
* **governance:** improve archival worker resilience ([50c501c](https://github.com/LerianStudio/matcher/commit/50c501c50d5a3af99422a53dab5f507c4be31ea3))
* **ci, tests:** Improve stability of integration and E2E tests ([37d6e74](https://github.com/LerianStudio/matcher/commit/37d6e74f3a243f6cac19fb55857a5ea5e2275abb))
* **e2e:** preserve key absence in fetcher config snapshot/restore ([a51d5f1](https://github.com/LerianStudio/matcher/commit/a51d5f1fd542ea094e9531bb92226e0862e0b14a))
* **bootstrap:** propagate context through logger sync ([cf3e909](https://github.com/LerianStudio/matcher/commit/cf3e90906b6cd7e42a78454df7d80c9956650b39))
* **exception:** reacquire failed idempotency keys and propagate sentinel errors ([0e0e46f](https://github.com/LerianStudio/matcher/commit/0e0e46f9293d204ae459192a5444ce100d91bf30))
* **security:** reject dev systemplane master key outside development and test ([c226921](https://github.com/LerianStudio/matcher/commit/c226921d08aa5d38ce52bc52a817a6fee6b3cc7d))
* **security:** reject URL-safe base64 for systemplane master key ([6319b16](https://github.com/LerianStudio/matcher/commit/6319b16beeaf1bfc91f20a85c4b33a770723b151))
* **fetcher-bridge:** remediate Gate 4 review findings across T-001..T-006 ([1a290ba](https://github.com/LerianStudio/matcher/commit/1a290bafdd946e6cb840a3d0fdec61a00d5203f9))
* **ci:** remove deprecated pr-validation parameters ([180e8bf](https://github.com/LerianStudio/matcher/commit/180e8bf3feabf75b2dced721e0eb49929e2fceea))
* **shared/postgres:** remove redundant nil check and add reverse config map ([cdbb2d9](https://github.com/LerianStudio/matcher/commit/cdbb2d9522c58f028f8b56df5d49bd969e5080c0))
* **lint:** remove stale nolint:gosec directive in e2e client ([76d76c2](https://github.com/LerianStudio/matcher/commit/76d76c294aa3d2d5a02846ff3273548934dfe4ca))
* **shared:** replace deprecated fasthttp Args.VisitAll with All iterator ([9f4c967](https://github.com/LerianStudio/matcher/commit/9f4c967fc7b6faa65d476c599a6a03480bf20637))
* **migrations:** replace DO block with plain SQL and strip comment semicolons ([8f03f5d](https://github.com/LerianStudio/matcher/commit/8f03f5ded9acd447010b6c5ed4f67b7b32818d29))
* **security:** require signed webhook payloads in production deployments ([7a5dad7](https://github.com/LerianStudio/matcher/commit/7a5dad7bff8ec4c51a6c058efa7fa89d91624a3f))
* **auth:** require tenant claims only when both auth and multi-tenant are enabled ([dad4227](https://github.com/LerianStudio/matcher/commit/dad4227ee79b3ead1660c6e21e2d6e51c83506b1))
* **reporting:** return 400 for invalid pagination cursors ([09ad599](https://github.com/LerianStudio/matcher/commit/09ad5994b59e35d2337c5a0b8026e0b64951851b))
* **tests:** rewire bridge readiness journey onto v5 systemplane helpers ([e26d62f](https://github.com/LerianStudio/matcher/commit/e26d62fb9af661e61b564bc05e06c958e4912dcf))
* **security:** scope comment deletion to its owning exception ([a3a40de](https://github.com/LerianStudio/matcher/commit/a3a40dea36a6a996c81aeab3a46523c8a3163f99))
* **lint:** scope gosec G117/G704 exclusions for OAuth + M2M false positives ([ab75029](https://github.com/LerianStudio/matcher/commit/ab7502920eb4258fa94f4f602413046be5195455))
* **shared:** skip body-hash idempotency fallback for PATCH requests ([5cbc2f7](https://github.com/LerianStudio/matcher/commit/5cbc2f76d6faea5394008fb156384ec60dd95450))
* **migrations:** strip semicolons from SQL comments ([047da45](https://github.com/LerianStudio/matcher/commit/047da457d1341bcb8d9ddef48ec97395439bf860))
* **discovery:** suppress false-positive gosec G704 on validated fetcher URL ([3401209](https://github.com/LerianStudio/matcher/commit/34012096399521377cadacc55318bce95a596e11))
* **lint:** suppress gosec G704 in e2e test client ([4d92b0c](https://github.com/LerianStudio/matcher/commit/4d92b0c2adb21a3308e0616dfcc0f92596c3e043))
* **tests:** swap deleted outbox postgres package for canonical v5 helper ([4d55b13](https://github.com/LerianStudio/matcher/commit/4d55b13b90d1ccbb95282f0113e5f171c4e5e3ae))
* **security:** tighten dev-mode allowlists for X-User-ID and trusted proxies ([33dcd2d](https://github.com/LerianStudio/matcher/commit/33dcd2dbd48bd58d2bcca7732a823952d986512d))
* **fee:** trim name whitespace in NewFeeSchedule ([b64f7a6](https://github.com/LerianStudio/matcher/commit/b64f7a66b82be8ad93fb0905e42f27eaa7b4ec26))
* **e2e:** use safe type assertion for listener address in mock Fetcher ([1618f62](https://github.com/LerianStudio/matcher/commit/1618f626bdc06d2fceface0609eaba7efc14e6eb))
* **security:** use structured logging in exception matching gateway ([ac58f7d](https://github.com/LerianStudio/matcher/commit/ac58f7dedf02583f5ac794389aa962eaad9b0a63))
* **exception:** warn-log on unmapped adjustment reason codes ([2c863aa](https://github.com/LerianStudio/matcher/commit/2c863aab68a96aee3bedccdc45773ae1e4c9debe))
* **config:** wrap match rule validation error with context ([4f1b2f1](https://github.com/LerianStudio/matcher/commit/4f1b2f1ffc60bc0cb7cb139d228de15d00e87e5d))


### Performance Improvements

* **audit-logs:** add composite index for tenant+entity+ordering queries ([04e9113](https://github.com/LerianStudio/matcher/commit/04e911361e59b355b601a7a5407f3b264fb5aa48))
* **m2m:** coalesce concurrent credential fetches via singleflight ([312b535](https://github.com/LerianStudio/matcher/commit/312b535db3e61cf19fa164501483da4952a5f28f))
* GOMEMLIMIT startup warning + parallel export-job cleanup ([f914316](https://github.com/LerianStudio/matcher/commit/f9143168bd0de1b69b3154eda0a7bce029a9d67d))
* **domain:** NewTransactionWithDonatedMetadata skips recursive clone ([67b8117](https://github.com/LerianStudio/matcher/commit/67b8117698278ee614f1473ce70c588d5f69a618))
* **exception:** reduce N+1 in bulk operations via FindByIDs preload ([c0d6d0c](https://github.com/LerianStudio/matcher/commit/c0d6d0c9cc64b2fcd37138856f0a5ad0718e5148))
* **bootstrap:** reject oversize requests via Content-Length before reading body ([815d7d8](https://github.com/LerianStudio/matcher/commit/815d7d8f8a76ac445c65a327d8365320bb5be4ed))
* **ingestion:** rewrite ExistsBulkBySourceAndExternalID with unnest arrays ([f4e5fa5](https://github.com/LerianStudio/matcher/commit/f4e5fa562fd7da5c2f3a30586c0f49699b45a922))
* **ingestion:** single-round-trip dedup via MarkSeenBulk Lua script ([fdf288c](https://github.com/LerianStudio/matcher/commit/fdf288c9d23502105ead6511d95b4d6168f71bb3))
* **http:** skip url.Values allocation when request has 0-1 query params ([333f0c9](https://github.com/LerianStudio/matcher/commit/333f0c93b8d475191c241d22c37f96f6256822f6))
* **archival:** stream partition export via io.Pipe to bound memory ([1445f74](https://github.com/LerianStudio/matcher/commit/1445f74fb7e848d9fffa47ae22da66d6be48419f))
* **bootstrap:** swap runtime CORS middleware to atomic.Pointer snapshot ([772e1d4](https://github.com/LerianStudio/matcher/commit/772e1d463b61e1f988b5c06dc2e57437405b438b))
* **exception:** switch exception repo reads to WithTenantReadQuery ([722534c](https://github.com/LerianStudio/matcher/commit/722534cad78a807f835f9bd0549ddba8db54dcd4))

## [1.3.0-beta.19](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.18...v1.3.0-beta.19) (2026-04-20)

## [1.3.0-beta.18](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.17...v1.3.0-beta.18) (2026-04-20)


### Features

* **bootstrap:** add dedicated admin rate-limit tier for /system plane ([2f65348](https://github.com/LerianStudio/matcher/commit/2f653480c13922f9b009be74c619feb4c3be2e76))
* **bootstrap:** apply global rate limit to systemplane admin API ([773b331](https://github.com/LerianStudio/matcher/commit/773b331bb0fd70bf6266784a58906219ea73489a))
* **reporting:** cursor-based pagination for export job listings ([0174364](https://github.com/LerianStudio/matcher/commit/0174364a545337710210837c5a07fd17efb4b563))
* **exception:** double actor-hash width and introduce SaltProvider hook ([1e7ea09](https://github.com/LerianStudio/matcher/commit/1e7ea096ba67ba718108a246b3882735a5d335d6))
* **systemplane:** drop orphan bootstrap-only rows and warn on future drift ([b4eeb91](https://github.com/LerianStudio/matcher/commit/b4eeb91f5ac68b2e20e8a010385c89026fbdd65c))
* **outbox:** enforce 1MiB payload cap and truncate oversized audit diffs ([d9dd847](https://github.com/LerianStudio/matcher/commit/d9dd847f55ca427ce5f6fc9413c0c07ca53def1a))
* **outbox:** expose typed status helpers from shared domain ([897931b](https://github.com/LerianStudio/matcher/commit/897931b57e8d28a817c44cd434d923e7fceccc8e))
* **bridge-worker:** fan tenants out up to BridgeTenantConcurrency per cycle ([c42df0b](https://github.com/LerianStudio/matcher/commit/c42df0bcce90b2f571eb715dc506dfb07e934d9a))
* **bootstrap:** guard runtime CORS and body-limit edits via systemplane validators ([ecbb65a](https://github.com/LerianStudio/matcher/commit/ecbb65aae3c77dc117b9be3adf5785f4a39316c2))
* **systemplane:** migrate admin API and config manager to lib-commons v5 ([dd9bcbf](https://github.com/LerianStudio/matcher/commit/dd9bcbf54fb9b20e0e9c952f55afb42e55c02039))
* **bootstrap:** reclassify auth, default-tenant, and outbox keys as bootstrap-only ([00f05a1](https://github.com/LerianStudio/matcher/commit/00f05a12b54fe1b12d06284f5ea3ce3bdac7095b))
* **outbox:** shared truncation helpers, single-marshal redesign, match-event ID guard ([0022fd5](https://github.com/LerianStudio/matcher/commit/0022fd53a3416d7157eac20dd439959b3641c788))
* **bootstrap:** shim removed v4 admin paths with 410 Gone ([a48cb64](https://github.com/LerianStudio/matcher/commit/a48cb647048e91dc991146ebc9a475a87285d34a))
* **auth:** split actor-mapping deanonymization into its own RBAC action ([179743d](https://github.com/LerianStudio/matcher/commit/179743db91cc3f9ae280cdf37e5a81bbff9cf52c))
* **governance:** surface audit truncation markers as first-class DTO fields ([09cb6ce](https://github.com/LerianStudio/matcher/commit/09cb6ce187fe54b41233fc921ce2fbd6d7101866))
* **bootstrap:** warn on missing GOMEMLIMIT in cgroup-capped containers ([ef0cbda](https://github.com/LerianStudio/matcher/commit/ef0cbda0234feac0a986bd113aa6140a4a4944dd))


### Bug Fixes

* address CodeRabbit review feedback on lib-commons v5 migration ([e5408a2](https://github.com/LerianStudio/matcher/commit/e5408a2258f4a682000a243048a6e6056962bdfd))
* address second round of CodeRabbit review feedback ([d27a6e3](https://github.com/LerianStudio/matcher/commit/d27a6e325521f45acdb65ff0a633e6b23ad3cfcb)), closes [#106](https://github.com/LerianStudio/matcher/issues/106)
* **dashboard:** align MatchRate to percentage scale across repo ([b851ad2](https://github.com/LerianStudio/matcher/commit/b851ad26537c53870de64753250f4c6a9cc3455b))
* **bootstrap:** apply typed-nil guard to ingestion and match publish helpers ([d068ab7](https://github.com/LerianStudio/matcher/commit/d068ab743ae970c3bd81b9e1c27a04b0704ba526))
* **test-harness:** avoid import cycle in outbox helper default-tenant wiring ([54a2ccf](https://github.com/LerianStudio/matcher/commit/54a2ccf9905a6fad5933f7c5c75b85200d81d15a))
* **security:** bind mock fetcher server to 127.0.0.1 ([a1264ed](https://github.com/LerianStudio/matcher/commit/a1264ed9ff96e351a2c0ba6917b1abd610bcf274))
* **security:** broaden SSRF deny-list with unspecified, multicast, and CGNAT ([2651d62](https://github.com/LerianStudio/matcher/commit/2651d62936bae6272e9182c37168be085aa0ab80))
* **docker:** bump Go builder to 1.26.2-alpine for stdlib CVE patches ([03e35ef](https://github.com/LerianStudio/matcher/commit/03e35efb770c229f7fb259a1d4298f78c8284d58))
* **bootstrap:** classify outbox payload cap and JSON errors as non-retryable ([48a376c](https://github.com/LerianStudio/matcher/commit/48a376ca03efa744a8897156fc1528e862d05035))
* **migrations:** drop CONCURRENTLY on partitioned audit_logs index ([d65e82d](https://github.com/LerianStudio/matcher/commit/d65e82de28393ed3400c9f007052aac3e6ce325a))
* **test:** eliminate race in ExactlyAtCap outbox payload test ([77ea37f](https://github.com/LerianStudio/matcher/commit/77ea37fc9816e5fb2766a7745bf1e117e0a04603))
* **security:** enforce minimum interval between reconciliation schedule firings ([66682b1](https://github.com/LerianStudio/matcher/commit/66682b1dcf43da2b548dae1066075fbbcc33409f))
* **discovery:** enforce uniqueness of Fetcher connection_id per source ([956ff49](https://github.com/LerianStudio/matcher/commit/956ff49531c653167648f93a0a32c4bd717a5a05))
* **tests:** extract rate-limit override into subpackage to break import cycle ([cfa0e0e](https://github.com/LerianStudio/matcher/commit/cfa0e0ef7ee8fa08852f396a9e67f8d1cf740f42))
* **security:** fail bootstrap when systemplane admin API mount fails ([d8d2d50](https://github.com/LerianStudio/matcher/commit/d8d2d5091308689e72604277635d66c8ec173409))
* **security:** fail closed when CreateAdjustment is missing authenticated user ([e9effc4](https://github.com/LerianStudio/matcher/commit/e9effc4f2886f7f837e79513ef4310462514f67a))
* **bootstrap:** guard defaultTenantDiscoverer against nil receiver and inner ([83a6150](https://github.com/LerianStudio/matcher/commit/83a6150537419411cfc7c96cf68e76491cdaa807))
* **test-harness:** guard nil dereferences in integration harnesses ([b367a88](https://github.com/LerianStudio/matcher/commit/b367a880493edf5242483f2a554f1653be6b531a))
* **nil-safety:** guard nil receivers and dependencies in bootstrap + exception gateway ([961b199](https://github.com/LerianStudio/matcher/commit/961b19957514a539ab7bcb1e2387127b8b6e2ee7))
* **nil-safety:** guard worker loops and publishers against nil elements ([019b809](https://github.com/LerianStudio/matcher/commit/019b8091f1c0bcf1565a4d4a31061fde4338a636))
* **scheduler:** harden SchedulerWorker.Stop against concurrent callers ([fae20db](https://github.com/LerianStudio/matcher/commit/fae20db02787c26bdbda75033de4d43f553aeec5))
* **reporting:** harden streaming iterators against nil rows ([dadeb19](https://github.com/LerianStudio/matcher/commit/dadeb19e01b35f9fe277d3139bb5dfa61aeca77a))
* **security:** reject dev systemplane master key outside development and test ([c226921](https://github.com/LerianStudio/matcher/commit/c226921d08aa5d38ce52bc52a817a6fee6b3cc7d))
* **security:** reject URL-safe base64 for systemplane master key ([6319b16](https://github.com/LerianStudio/matcher/commit/6319b16beeaf1bfc91f20a85c4b33a770723b151))
* **migrations:** replace DO block with plain SQL and strip comment semicolons ([8f03f5d](https://github.com/LerianStudio/matcher/commit/8f03f5ded9acd447010b6c5ed4f67b7b32818d29))
* **security:** require signed webhook payloads in production deployments ([7a5dad7](https://github.com/LerianStudio/matcher/commit/7a5dad7bff8ec4c51a6c058efa7fa89d91624a3f))
* **tests:** rewire bridge readiness journey onto v5 systemplane helpers ([e26d62f](https://github.com/LerianStudio/matcher/commit/e26d62fb9af661e61b564bc05e06c958e4912dcf))
* **security:** scope comment deletion to its owning exception ([a3a40de](https://github.com/LerianStudio/matcher/commit/a3a40dea36a6a996c81aeab3a46523c8a3163f99))
* **lint:** scope gosec G117/G704 exclusions for OAuth + M2M false positives ([ab75029](https://github.com/LerianStudio/matcher/commit/ab7502920eb4258fa94f4f602413046be5195455))
* **tests:** swap deleted outbox postgres package for canonical v5 helper ([4d55b13](https://github.com/LerianStudio/matcher/commit/4d55b13b90d1ccbb95282f0113e5f171c4e5e3ae))
* **security:** tighten dev-mode allowlists for X-User-ID and trusted proxies ([33dcd2d](https://github.com/LerianStudio/matcher/commit/33dcd2dbd48bd58d2bcca7732a823952d986512d))
* **fee:** trim name whitespace in NewFeeSchedule ([b64f7a6](https://github.com/LerianStudio/matcher/commit/b64f7a66b82be8ad93fb0905e42f27eaa7b4ec26))
* **security:** use structured logging in exception matching gateway ([ac58f7d](https://github.com/LerianStudio/matcher/commit/ac58f7dedf02583f5ac794389aa962eaad9b0a63))
* **exception:** warn-log on unmapped adjustment reason codes ([2c863aa](https://github.com/LerianStudio/matcher/commit/2c863aab68a96aee3bedccdc45773ae1e4c9debe))


### Performance Improvements

* **audit-logs:** add composite index for tenant+entity+ordering queries ([04e9113](https://github.com/LerianStudio/matcher/commit/04e911361e59b355b601a7a5407f3b264fb5aa48))
* **m2m:** coalesce concurrent credential fetches via singleflight ([312b535](https://github.com/LerianStudio/matcher/commit/312b535db3e61cf19fa164501483da4952a5f28f))
* GOMEMLIMIT startup warning + parallel export-job cleanup ([f914316](https://github.com/LerianStudio/matcher/commit/f9143168bd0de1b69b3154eda0a7bce029a9d67d))
* **domain:** NewTransactionWithDonatedMetadata skips recursive clone ([67b8117](https://github.com/LerianStudio/matcher/commit/67b8117698278ee614f1473ce70c588d5f69a618))
* **exception:** reduce N+1 in bulk operations via FindByIDs preload ([c0d6d0c](https://github.com/LerianStudio/matcher/commit/c0d6d0c9cc64b2fcd37138856f0a5ad0718e5148))
* **bootstrap:** reject oversize requests via Content-Length before reading body ([815d7d8](https://github.com/LerianStudio/matcher/commit/815d7d8f8a76ac445c65a327d8365320bb5be4ed))
* **ingestion:** rewrite ExistsBulkBySourceAndExternalID with unnest arrays ([f4e5fa5](https://github.com/LerianStudio/matcher/commit/f4e5fa562fd7da5c2f3a30586c0f49699b45a922))
* **ingestion:** single-round-trip dedup via MarkSeenBulk Lua script ([fdf288c](https://github.com/LerianStudio/matcher/commit/fdf288c9d23502105ead6511d95b4d6168f71bb3))
* **http:** skip url.Values allocation when request has 0-1 query params ([333f0c9](https://github.com/LerianStudio/matcher/commit/333f0c93b8d475191c241d22c37f96f6256822f6))
* **archival:** stream partition export via io.Pipe to bound memory ([1445f74](https://github.com/LerianStudio/matcher/commit/1445f74fb7e848d9fffa47ae22da66d6be48419f))
* **bootstrap:** swap runtime CORS middleware to atomic.Pointer snapshot ([772e1d4](https://github.com/LerianStudio/matcher/commit/772e1d463b61e1f988b5c06dc2e57437405b438b))
* **exception:** switch exception repo reads to WithTenantReadQuery ([722534c](https://github.com/LerianStudio/matcher/commit/722534cad78a807f835f9bd0549ddba8db54dcd4))

## [Unreleased]


### BREAKING CHANGES

* **systemplane:** admin HTTP API paths changed from `/v1/system/configs[...]` and `/v1/system/settings[...]` to the canonical lib-commons v5 layout at `/system/:namespace` (list) and `/system/:namespace/:key` (get/set). The `/schema`, `/history`, and `/reload` sub-endpoints are REMOVED ENTIRELY; schema metadata is now returned inline in list responses, history is available only via audit logs, and reload is no longer an HTTP-exposed operation (v5 auto-subscribes). Write verb changed from `PATCH` to `PUT`. Per-key writes replace bulk PATCH. Clients (including the matcher console) must update in lockstep; a short-lived `410 Gone` shim returns a structured JSON body (`code: GONE`, `hint`, `removal`) for every removed v4 path so stale tooling fails loud instead of silent-404ing. The shim is scheduled for removal four weeks after the 2026-04-18 cutover.

* **systemplane:** per-tenant scope removed. v4 supported `Subject{Scope: ScopeTenant, SubjectID}` allowing tenants to override rate_limit, idempotency TTLs, etc. individually. v5 collapses to flat `namespace + key` — any tenant-scoped overrides written against v4 are silently ignored after upgrade. Audit production systemplane tables for tenant-scoped rows before deploying.

* **systemplane credentials:** the following systemplane keys were REMOVED — they were bootstrap-only (connection identity / credentials) but were misleadingly registered as runtime-mutable. Editing them via admin API had no effect; rotation requires a restart and env-var change:
  - postgres.primary_*, postgres.replica_*, postgres.connect_timeout_sec, postgres.migrations_path
  - redis.host, redis.master_name, redis.password, redis.db, redis.protocol, redis.tls, redis.ca_cert, redis.dial_timeout_ms
  - rabbitmq.url, rabbitmq.host, rabbitmq.port, rabbitmq.user, rabbitmq.password, rabbitmq.vhost, rabbitmq.health_url, rabbitmq.allow_insecure_health_check
  - app.log_level (LOG_LEVEL is now documented as bootstrap-only)

* **auth:** lib-auth v2 → v3 migration. Verifier pattern changed from interface-based to function-type. `SafeError` signature grew from 3 to 5 parameters (adds ctx + production flag). `ParseAndVerify*` now returns `(resourceID, tenantID, error)` tuple.

* **outbox:** bespoke dispatcher (4155 lines) and PostgreSQL outbox repository (935 lines) DELETED — replaced by canonical `lib-commons/v5/commons/outbox.Dispatcher` and `/outbox/postgres.Repository`. Matcher-specific `defaultTenantDiscoverer` wrapper preserves critical default-tenant dispatch behavior. `NewOutboxEvent` now enforces 1 MiB payload cap + JSON validity.

## [1.3.0-beta.17](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.16...v1.3.0-beta.17) (2026-04-18)


### Bug Fixes

* **ci, tests:** Improve stability of integration and E2E tests ([37d6e74](https://github.com/LerianStudio/matcher/commit/37d6e74f3a243f6cac19fb55857a5ea5e2275abb))

## [1.3.0-beta.16](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.15...v1.3.0-beta.16) (2026-04-17)


### Features

* **fetcher-bridge:** add trusted stream intake boundary (T-001) ([7a74051](https://github.com/LerianStudio/matcher/commit/7a74051dea2735500aaad6b53495e0576c8ebbe2))
* **fetcher-bridge:** automatic completed-extraction bridging worker (T-003) ([efa0bbe](https://github.com/LerianStudio/matcher/commit/efa0bbeef578390561db8522164846c9122ca4a4))
* **fetcher-bridge:** retention operations with converging sweep (T-006) ([b148cee](https://github.com/LerianStudio/matcher/commit/b148cee9ad30fd626b7e66f07d4a20f41f5ee130))
* **fetcher-bridge:** retry-safe failure and staleness control (T-005) ([15daeb8](https://github.com/LerianStudio/matcher/commit/15daeb8068c5bba985b22f38fae0cdbdbe859d6d))
* **migrations:** tighten bridge eligibility index and reindex concurrently ([51bb683](https://github.com/LerianStudio/matcher/commit/51bb6831a9ec65d2fb844359889371b7ffcf1986))
* **fetcher-bridge:** truthful operational readiness projection (T-004) ([f0f32ca](https://github.com/LerianStudio/matcher/commit/f0f32ca81a695469cbdaea35876cd6ddb967bc2c))
* **fetcher-bridge:** verified artifact retrieval and custody (T-002) ([6e2dad0](https://github.com/LerianStudio/matcher/commit/6e2dad0f0356a7b4cb04debb212f19d827a76fc3))


### Bug Fixes

* **fetcher-bridge:** remediate Gate 4 review findings across T-001..T-006 ([1a290ba](https://github.com/LerianStudio/matcher/commit/1a290bafdd946e6cb840a3d0fdec61a00d5203f9))
* **migrations:** strip semicolons from SQL comments ([047da45](https://github.com/LerianStudio/matcher/commit/047da457d1341bcb8d9ddef48ec97395439bf860))

## [1.3.0-beta.15](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.14...v1.3.0-beta.15) (2026-04-15)


### Features

* **discovery:** rewrite Fetcher integration with OAuth2 and schema qualification ([6cffce2](https://github.com/LerianStudio/matcher/commit/6cffce20deb9b79b517942445bba9c7165d04006))


### Bug Fixes

* **config:** wrap match rule validation error with context ([4f1b2f1](https://github.com/LerianStudio/matcher/commit/4f1b2f1ffc60bc0cb7cb139d228de15d00e87e5d))

## [1.3.0-beta.14](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.13...v1.3.0-beta.14) (2026-04-14)


### Bug Fixes

* **discovery:** align Fetcher client paths with actual service routes ([e11c23e](https://github.com/LerianStudio/matcher/commit/e11c23e6fd76937ca8b01bfb19cb6d59c34c3640))
* **lint:** remove stale nolint:gosec directive in e2e client ([76d76c2](https://github.com/LerianStudio/matcher/commit/76d76c294aa3d2d5a02846ff3273548934dfe4ca))

## [1.3.0-beta.13](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.12...v1.3.0-beta.13) (2026-04-09)


### Bug Fixes

* **auth:** require tenant claims only when both auth and multi-tenant are enabled ([dad4227](https://github.com/LerianStudio/matcher/commit/dad4227ee79b3ead1660c6e21e2d6e51c83506b1))
* **lint:** suppress gosec G704 in e2e test client ([4d92b0c](https://github.com/LerianStudio/matcher/commit/4d92b0c2adb21a3308e0616dfcc0f92596c3e043))

## [1.3.0-beta.12](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.11...v1.3.0-beta.12) (2026-04-08)


### Features

* **bootstrap:** add migration preflight guards ([59b43b2](https://github.com/LerianStudio/matcher/commit/59b43b297e3cabfcbba0efc34fe5d3c9fdd0e0dc))

## [1.3.0-beta.11](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.10...v1.3.0-beta.11) (2026-04-07)


### Bug Fixes

* **ci:** remove deprecated pr-validation parameters ([180e8bf](https://github.com/LerianStudio/matcher/commit/180e8bf3feabf75b2dced721e0eb49929e2fceea))

## [1.3.0-beta.10](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.9...v1.3.0-beta.10) (2026-04-04)

## [1.3.0-beta.9](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.8...v1.3.0-beta.9) (2026-04-04)


### Bug Fixes

* **bootstrap:** flatten config patch apply flow ([af6e886](https://github.com/LerianStudio/matcher/commit/af6e88684d14e211d632b048245fb9bc55d84c05))

## [1.3.0-beta.8](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.7...v1.3.0-beta.8) (2026-04-04)


### Features

* **auth:** add Casdoor seed generator ([3877651](https://github.com/LerianStudio/matcher/commit/3877651c6563f136cbb771386e1845d55be630ef))
* **bootstrap:** add runtime settings layer ([fa73f05](https://github.com/LerianStudio/matcher/commit/fa73f059b226a50ffd4e267ad5da17ea2f7aebf0))

## [1.3.0-beta.7](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.6...v1.3.0-beta.7) (2026-04-02)


### Bug Fixes

* address CodeRabbit review - clarify lib-commons refs, align Go version, fix Swagger path ([ce8e023](https://github.com/LerianStudio/matcher/commit/ce8e023ac0c75d081c7c82bfba9f04d932c54d92))
* address CodeRabbit review - remove stale toolchain ref, add Swagger path ([12ed48c](https://github.com/LerianStudio/matcher/commit/12ed48c4910dc8ed0d9748afe9c33ca802ed2515))

## [1.3.0-beta.6](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.5...v1.3.0-beta.6) (2026-04-01)

## [1.3.0-beta.5](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.4...v1.3.0-beta.5) (2026-04-01)

## [1.3.0-beta.4](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.3...v1.3.0-beta.4) (2026-03-31)


### Features

* **exception:** improve dispatch error handling and migrate external_system to varchar ([fe15a8a](https://github.com/LerianStudio/matcher/commit/fe15a8af0cf788c963dda965de513a2f158e4d1d))
* **governance:** return persisted entity from actor mapping upsert ([294d3c5](https://github.com/LerianStudio/matcher/commit/294d3c5091a9a22336b2bc3678799d4a93aad245))


### Bug Fixes

* apply CodeRabbit review fixes ([ac337ac](https://github.com/LerianStudio/matcher/commit/ac337acac89168acd8f3e860310221023bdfa382))
* apply CodeRabbit review fixes (round 2) ([9c44cf7](https://github.com/LerianStudio/matcher/commit/9c44cf7620b0c14cd27fc66b03c2f65f4836f1e0))
* apply CodeRabbit review fixes (round 3) ([ef8c4c7](https://github.com/LerianStudio/matcher/commit/ef8c4c7a29cfb4eefaea2ffbb0bbad43b6afea17))
* **e2e:** preserve key absence in fetcher config snapshot/restore ([a51d5f1](https://github.com/LerianStudio/matcher/commit/a51d5f1fd542ea094e9531bb92226e0862e0b14a))
* **reporting:** return 400 for invalid pagination cursors ([09ad599](https://github.com/LerianStudio/matcher/commit/09ad5994b59e35d2337c5a0b8026e0b64951851b))
* **e2e:** use safe type assertion for listener address in mock Fetcher ([1618f62](https://github.com/LerianStudio/matcher/commit/1618f626bdc06d2fceface0609eaba7efc14e6eb))

## [1.3.0-beta.3](https://github.com/LerianStudio/matcher/compare/v1.3.0-beta.2...v1.3.0-beta.3) (2026-03-30)


### Features

* **m2m:** add M2M credential provider with AWS Secrets Manager ([d658c86](https://github.com/LerianStudio/matcher/commit/d658c860016b61c599ed0dff4dfe323abc70582e))
* **multi-tenant:** cache tenant middleware with TenantCache and RWMutex ([cac4412](https://github.com/LerianStudio/matcher/commit/cac4412de5c44c768a7e3c3320872892454083f2))
* **multi-tenant:** expand tenancy config with Redis, caching, timeout, and M2M settings ([4c02f5e](https://github.com/LerianStudio/matcher/commit/4c02f5e0f074059aebc583690cb782e26ac38f5a))
* **discovery:** integrate M2M credentials into Fetcher client ([623a01d](https://github.com/LerianStudio/matcher/commit/623a01df2b4f21e52c2c21c18dbb318dd8041322))


### Bug Fixes

* **shared:** address CI lint and CodeRabbit review findings ([f1b3cb4](https://github.com/LerianStudio/matcher/commit/f1b3cb400a7b8e3fad0d8c1d66a71045a46379ce))

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
