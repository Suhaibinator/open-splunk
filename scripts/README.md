# Build scripts

Protobuf generation, release builds, migration checks, and packaging automation belong here. Release automation must build the Next.js static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.
