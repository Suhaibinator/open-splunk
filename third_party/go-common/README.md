# go-common

A shared Go backend library and TypeScript workspace for authentication, users,
organizations, attachments, chat, administration, and observability. The project
is in alpha: consumers should expect breaking changes between releases.

## Overview

`go-common` provides a complete foundation for backend services including:
- Multi-provider authentication (email + OAuth)
- Database and object storage abstractions
- Rate limiting and security middleware
- File upload and processing pipelines
- Metrics, logging, and observability
- Type-safe API contracts with Protocol Buffers

## Key Features

### 🏗️ Layered Architecture
- **API Layer**: HTTP endpoints with Protocol Buffer serialization
- **Service Layer**: Business logic with clean interfaces
- **Persistence Layer**: Database (PostgreSQL) and object storage (MinIO)
- Dependency injection and interface-based design throughout

### 🔐 Authentication & Security
- **Email Authentication**: Registration, login, password reset with verification
- **OAuth Support**: any OAuth2/OIDC provider; adapters for Google, GitHub, Microsoft, Apple, Okta, Discord, LinkedIn, Facebook
- **Token Management**: Secure token generation and caching
- **Configurable Permissions**: Operation-based authorization with configurable user type thresholds.
- **Rate Limiting**: Configurable per-action attempt windows with retry-after durations
- **Input Sanitization**: HTML stripping, email validation, content filtering

### 📁 File Management
- **Direct Uploads**: Presigned URLs for client-side uploads
- **Async Processing**: Background validation, promotion, recovery, and cleanup workers
- **Lifecycle Management**: Automatic cleanup of temporary files
- **Profile Pictures**: Special handling for user avatars

### 📊 Observability
- **Prometheus Metrics**: Built-in backend instrumentation
- **Structured Logging**: Request tracing and correlation IDs
- **Request Logger**: Legacy low-level SQLite middleware, not installed by default
- **Frontend Telemetry**: Grafana Faro integration

### 🔧 Developer Experience
- **Generic Types**: Type-safe handlers with custom user models
- **Error Handling**: Consistent error types and HTTP mapping
- **Configuration**: A reusable typed fragment composed into the importing application's release-atomic KMS root
- **TypeScript Types**: Auto-generated types from Protocol Buffers

## Quick Start

Install the Go module, or the two independently published frontend packages:

The repository is private. Go needs `GOPRIVATE=github.com/Suhaibinator/*` and
Git credentials for GitHub. The npm packages are on GitHub Packages, so map the
`@suhaibinator` scope to `https://npm.pkg.github.com` and authenticate with a
token that has `read:packages`. See the
[development prerequisites](./docs/internal/development.md).

```bash
go get github.com/Suhaibinator/go-common@v1.0.0
npm install @suhaibinator/go-common-core @suhaibinator/go-common-ui react react-dom
```

Backend applications hold one `config.Provider`, narrow it for each facade,
and initialize persistence → service → API. Use the
[backend startup guide](./docs/backend/README.md) for the current wiring and
[managed configuration guide](./docs/configuration/developers.md) when the
application owns a KMS-backed store. Do not copy older examples that pass a
`*config.Config` directly to facade constructors. Logging and sanitization also
accept their narrowed providers directly: `logger.Initialize(cfg.Logging())` and
`common_sanitizer.NewHandler(cfg.Sanitization(), logger)`.

The typed narrowings — `cfg.Service()`, `cfg.API()`, `cfg.Persistence()`,
`cfg.Sanitization()`, `cfg.Converter()`, `cfg.Metrics()`, `cfg.Logging()`,
`cfg.Genai()`, `cfg.Realtime()` — preserve per-facade compile-time scoping: each
constructor can only read the fields its aggregate view declares. Restart-bound
settings are captured once at construction. Hot boundaries (rate limits, user
search, session refresh, signup/auth policy, chat limits, organization policy,
attachment runtime policy, WebSocket handshake policy, log level, and more)
sample the provider once per operation automatically, so a provider backed by a
live store makes them hot with no extra wiring. To give one component a
different source, pass a different provider to its constructor. See
[Managed configuration for developers](./docs/configuration/developers.md).

## Documentation

- [Documentation hub](./docs/README.md) - the full docs index
- [Managed configuration for developers](./docs/configuration/developers.md)
- [Managed configuration for operators](./docs/configuration/operators.md)

### For Consumers

- [Backend integration](./docs/backend/README.md) — wiring and supported feature guides
- [Backend workflow reference](./docs/backend/external/api.md) — selected HTTP workflows and examples
- [Frontend integration](./docs/frontend/README.md) — using `@suhaibinator/go-common-core` and `@suhaibinator/go-common-ui`
- [Runnable Next.js example](./typescript/examples/next-app/README.md)

Consumer integration contracts live under `docs/backend/external/` and
`docs/frontend/external/`.

### For go-common Contributors

- [Development and testing](./docs/internal/development.md)
- [Backend architecture](./docs/backend/internal/architecture.md)
- [Frontend architecture](./docs/frontend/internal/architecture.md)
- [AGENTS.md](./AGENTS.md) — authoritative repository guidance

Contributor implementation notes live under `internal/`; plans and reviews are
historical engineering records, not consumer API contracts.

## Requirements

- Go 1.27.2 or newer (the minimum declared in `go.mod`)
- PostgreSQL (CI validates PostgreSQL 16)
- MinIO or S3-compatible storage

Rate limiting is in-process; there is no Redis or other external cache dependency.

## Contributing

See the [development and testing guide](./docs/internal/development.md) for
tool versions, private-module access, real-Postgres test behavior, and CI-parity
commands. Repository-specific coding guidance is in [AGENTS.md](./AGENTS.md).

## License

No repository license file is checked in. The TypeScript package manifests
declare `ISC`, while distribution currently targets internal infrastructure.
The license text and scope for the Go module and npm packages need to be
established before a public release.
