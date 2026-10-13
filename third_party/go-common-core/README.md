# GoCommon frontend client

`suhaibinator-go-common-core-1.0.0.tgz` contains the actual GoCommon core package,
compiled from the source revision in `go-common-core-provenance.json`. The package
declares the ISC license. OpenSplunk uses its cookie-backed client and generated
protobuf contracts; it does not implement an OAuth protocol.

The public npm registry does not publish this package. The checked-in archive
makes builds reproducible until a matching registry release is available.
No GoCommon UI or Tailwind dependency is included. Node16 compilation keeps the
same client source usable by both Next.js and the CommonJS frontend test runner.

To rebuild from a GoCommon checkout, install its core workspace dependencies,
then run `node scripts/vendor-go-common-core.mjs /path/to/go-common` from the
OpenSplunk root. Run `npm install --ignore-scripts ./third_party/go-common-core/suhaibinator-go-common-core-1.0.0.tgz`
to update the installed package and lockfile integrity. The script needs the
repository's pinned Buf and ts-proto tools and GoCommon's TypeScript compiler.
