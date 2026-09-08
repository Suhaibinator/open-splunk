Recorded TDD failures from this implementation session

Original starting revision: a8092b5111b98c3d18fcacb6e1a6b49918f1e030.
The dedicated test agents wrote expected values independently. The fixed/text
red logs were captured after format recognition and a decoder scaffold existed,
before the native implementations. They demonstrate decoding failures beyond
configuration rejection. Fixed-e2e and fixed-tail logs are subsequent runs
with local sockets permitted, reaching delivery/rejection/checkpoint assertions.
The socket-permission failures included in fixed-red are infrastructure failures,
not evidence of a parser defect. Original tests were authored in the shared
working tree, so these intermediate states do not each have a separate commit.

Durability-current records the independent post-recognition durability run,
including explicit-policy secrets reaching serialized/WAL events. It also
contains the then-current oversized-framing expectation; the complete artifact
output is preserved, not every historical failure is claimed as a new parser
bug. Native-redaction and native-embedded logs isolate reproduced opt-in policy
leaks, followed by the fixes and passing race/integration gates recorded in
../verification.txt. No-policy preservation has separate independent coverage.

Later logs retain reproduced Apache empty-user, logfmt surrogate, strict YAML
presence, original timestamp suffix, negative-one-second offset fuzz and
same-role alias failures before their respective fixes. Review source revisions
and findings are recorded in ../reviews.txt. Payload values here are synthetic
fixtures; all outputs are copied verbatim from the recorded local runs.
