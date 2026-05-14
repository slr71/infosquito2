# infosquito2 Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modernize infosquito2: Go 1.26, multi-stage distroless Dockerfile, updated dependencies (including messaging v9→v12 with streadway→amqp091 swap), and a clean golangci-lint run.

**Architecture:** Four self-contained tasks executed in order. Tasks 1–2 handle Go toolchain + dependency upgrades and resolve the messaging library's amqp package swap (`github.com/streadway/amqp` → `github.com/rabbitmq/amqp091-go`). Task 3 introduces a multi-stage Dockerfile that builds with golang:1.26 and ships on `gcr.io/distroless/static-debian13:nonroot`. Task 4 cleans up all 10 golangci-lint findings: 7 errcheck warnings (handled with a single generic `logIfErr` helper in a new `errcheck.go` file) and 3 staticcheck QF1003 warnings (tagged-switch refactors).

**Tech Stack:** Go 1.26, golangci-lint, distroless static-debian13, RabbitMQ/AMQP 0.9.1, Elasticsearch v7 client, OpenTelemetry, lib/pq, viper.

---

## File Structure

- **Modify:** `go.mod` — bump `go 1.25.0` → `go 1.26`; update direct deps (`messaging/v9` → `messaging/v12`, `otelutils v0.0.3` → `v0.0.6`, `lib/pq`, `sirupsen/logrus`, `otel/contrib/...otelhttp`).
- **Modify:** `go.sum` — regenerated via `go mod tidy`.
- **Modify:** `main.go` — import `github.com/cyverse-de/messaging/v12` instead of `v9`; replace `github.com/streadway/amqp` with `github.com/rabbitmq/amqp091-go` (alias `amqp`).
- **Modify:** `Dockerfile` — multi-stage build: builder on `golang:1.26`, final image on `gcr.io/distroless/static-debian13:nonroot`.
- **Create:** `errcheck.go` — single generic helper `logIfErr(fn func() error, what string)` that calls `fn` and logs at error level if non-nil.
- **Create:** `errcheck_test.go` — unit test for the helper.
- **Modify:** `reindex.go` — replace 5 errcheck call-sites with `logIfErr` and convert 3 `if/else-if` chains comparing `classification` / `docType` into `switch` statements.
- **Modify:** `tags.go` — replace 2 errcheck call-sites with `logIfErr`.

---

## Task 1: Bump Go toolchain to 1.26

**Files:**
- Modify: `go.mod` (line 3)

- [ ] **Step 1: Read current go.mod**

Run: `head -3 go.mod`
Expected output includes `go 1.25.0`.

- [ ] **Step 2: Change Go directive**

Edit `go.mod` line 3:

```
go 1.26
```

(Replace `go 1.25.0` with `go 1.26`.)

- [ ] **Step 3: Verify the project still builds and tests pass**

Run: `go build ./... && go test ./...`
Expected: build succeeds; all tests in `document_test.go` pass.

- [ ] **Step 4: Commit**

```bash
git add go.mod
git commit -m "bump Go toolchain to 1.26"
```

---

## Task 2: Update dependencies (incl. messaging v9 → v12)

The messaging library v12 replaces the deprecated `github.com/streadway/amqp` with `github.com/rabbitmq/amqp091-go`. The `messaging.Client` API surface used by infosquito2 (`NewClient`, `Close`, `SetupPublishing`, `PurgeQueue`, `Listen`, `AddConsumerMulti`, `PublishContext`) is unchanged. Only the `amqp.Delivery` type referenced in handler signatures comes from the new package. The fields/methods we use on `Delivery` (`RoutingKey`, `Redelivered`, `Ack`, `Reject`) exist identically on `amqp091.Delivery`.

**Files:**
- Modify: `main.go` (imports block, lines 19–22)
- Modify: `go.mod`, `go.sum` (regenerated)

- [ ] **Step 1: Update direct dependencies via `go get`**

Run each in order:

```bash
go get github.com/cyverse-de/messaging/v12@v12.0.1
go get github.com/cyverse-de/go-mod/otelutils@v0.0.6
go get github.com/cyverse-de/configurate@latest
go get github.com/sirupsen/logrus@v1.9.4
go get github.com/lib/pq@v1.12.0
go get github.com/spf13/viper@v1.21.0
go get go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.68.0
go get github.com/rabbitmq/amqp091-go@v1.10.0
```

Note: `messaging/v12` will pull in `rabbitmq/amqp091-go` as an indirect dep, but we add it explicitly because `main.go` imports it directly.

- [ ] **Step 2: Drop the messaging/v9 module reference**

Remove the explicit `messaging/v9` line (the v12 `go get` above adds the new line but does not remove the old). After `go get`, run:

```bash
grep "messaging" go.mod
```

Expected: only `github.com/cyverse-de/messaging/v12` should appear. If `messaging/v9` is still listed, run `go mod edit -droprequire github.com/cyverse-de/messaging/v9`.

- [ ] **Step 3: Drop the streadway/amqp dependency**

Run:

```bash
go mod edit -droprequire github.com/streadway/amqp
```

(Will become unused once the import is swapped in Step 4. Safe to drop now since the `go mod tidy` in Step 5 will re-add if anything still references it — nothing should after Step 4.)

- [ ] **Step 4: Swap amqp import and messaging version in `main.go`**

Edit the imports block in `main.go` (currently lines 19–22):

Old:
```go
	"github.com/cyverse-de/messaging/v9"
	"github.com/streadway/amqp"
```

New:
```go
	"github.com/cyverse-de/messaging/v12"
	amqp "github.com/rabbitmq/amqp091-go"
```

The `amqp` alias keeps the rest of `main.go` (which references `amqp.Delivery`) untouched.

- [ ] **Step 5: Tidy and verify**

Run:

```bash
go mod tidy
go build ./...
go test ./...
```

Expected: build succeeds; all tests pass; no missing/unused module errors. If `go build` reports type incompatibilities on `amqp.Delivery` between our handler signatures and `messaging.MessageHandler`, both packages now resolve to `rabbitmq/amqp091-go.Delivery` and this should not happen — if it does, double-check the import alias spelled `amqp` exactly.

- [ ] **Step 6: Run golangci-lint**

Run: `golangci-lint run`
Expected: the same 10 pre-existing warnings (7 errcheck, 3 staticcheck) — and **no new warnings**. New warnings here mean the upgrades introduced something that needs investigation.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum main.go
git commit -m "update deps: messaging v9->v12, otelutils v0.0.6, otelhttp v0.68.0, viper v1.21, logrus v1.9.4, drop streadway/amqp"
```

---

## Task 3: Multi-stage Dockerfile on distroless

**Files:**
- Modify: `Dockerfile` (full rewrite)

- [ ] **Step 1: Replace Dockerfile contents**

Write to `Dockerfile`:

```dockerfile
FROM golang:1.26 AS build

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

ENV CGO_ENABLED=0

WORKDIR /src/infosquito2

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath \
    -ldflags="-s -w -X main.appver=$version -X main.gitref=$git_commit" \
    -o /out/infosquito2 .

FROM gcr.io/distroless/static-debian13:nonroot

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/infosquito2"
LABEL org.label-schema.version="$descriptive_version"

COPY --from=build /out/infosquito2 /bin/infosquito2

USER nonroot:nonroot

EXPOSE 60000
ENTRYPOINT ["/bin/infosquito2"]
CMD ["--help"]
```

Notes about deliberate choices:
- ARG declarations appear in both stages because each stage has its own scope. We need them in the build stage for the ldflags and in the final stage for the LABELs.
- `-trimpath -ldflags="-s -w ..."` matches the pattern used by sibling repo `permissions` and produces a smaller, reproducible binary.
- The previous Dockerfile used `go install`; we switch to `go build -o` because distroless has no `$GOPATH/bin`.
- The `var appver` / `var gitref` ldflag variables exist in `main.go` only if they're declared there. Inspect `main.go`: if neither `appver` nor `gitref` is declared, drop `-X main.appver=...` and `-X main.gitref=...` from `-ldflags`. (Per a quick scan, `main.go` does not currently declare these — the original Dockerfile's `-X` flags were no-ops. Drop them.)

After writing the file, confirm by reading `main.go` for `appver`/`gitref` declarations:

Run: `grep -n "appver\|gitref" main.go`
Expected: no matches → drop the `-X main.appver=...` and `-X main.gitref=...` segments from the build line, leaving:

```dockerfile
RUN go build -trimpath -ldflags="-s -w" -o /out/infosquito2 .
```

If `grep` does find them, keep the `-X` flags as written above.

- [ ] **Step 2: Build the image locally**

Run:

```bash
docker build -t infosquito2:test .
```

(If `docker` is not available, use `podman build -t infosquito2:test .` — the `de/CLAUDE.md` notes podman is preferred when available.)

Expected: build succeeds; final image is small (~20MB) because it's distroless static.

- [ ] **Step 3: Smoke-test the image**

Run:

```bash
docker run --rm infosquito2:test --help
```

Expected: usage output printed (the `--help` flag is handled by Go's `flag` package).

- [ ] **Step 4: Commit**

```bash
git add Dockerfile
git commit -m "switch to multi-stage Dockerfile on distroless/static-debian13:nonroot"
```

---

## Task 4: Fix golangci-lint findings

This task adds a generic errcheck helper, then uses it everywhere, then converts three if/else-if chains to tagged switches.

### Task 4a: Add `logIfErr` helper with test

**Files:**
- Create: `errcheck.go`
- Create: `errcheck_test.go`

- [ ] **Step 1: Write the failing test**

Create `errcheck_test.go`:

```go
package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLogIfErr_CallsFunctionAndLogsError(t *testing.T) {
	var buf bytes.Buffer
	origOut := logrus.StandardLogger().Out
	logrus.SetOutput(&buf)
	defer logrus.SetOutput(origOut)

	called := false
	logIfErr(func() error {
		called = true
		return errors.New("boom")
	}, "closing widget")

	if !called {
		t.Fatal("logIfErr did not invoke the supplied function")
	}
	out := buf.String()
	if !strings.Contains(out, "closing widget") || !strings.Contains(out, "boom") {
		t.Fatalf("expected log to contain context and error, got: %q", out)
	}
}

func TestLogIfErr_NoLogOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	origOut := logrus.StandardLogger().Out
	logrus.SetOutput(&buf)
	defer logrus.SetOutput(origOut)

	logIfErr(func() error { return nil }, "closing widget")

	if buf.Len() != 0 {
		t.Fatalf("expected no log output on success, got: %q", buf.String())
	}
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `go test -run TestLogIfErr ./...`
Expected: FAIL with `undefined: logIfErr`.

- [ ] **Step 3: Create the helper**

Create `errcheck.go`:

```go
package main

func logIfErr(fn func() error, what string) {
	if err := fn(); err != nil {
		log.Errorf("Failed %s: %s", what, err)
	}
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `go test -run TestLogIfErr ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add errcheck.go errcheck_test.go
git commit -m "add logIfErr helper for deferred error-returning calls"
```

### Task 4b: Replace errcheck call-sites in reindex.go and tags.go

**Files:**
- Modify: `reindex.go` (lines 218, 283, 410, 416, 448)
- Modify: `tags.go` (lines 67, 140)

- [ ] **Step 1: Confirm lint warnings exist before the change**

Run: `golangci-lint run 2>&1 | grep errcheck`
Expected: 7 errcheck findings as listed in the original output.

- [ ] **Step 2: Edit `reindex.go` line 218**

Old:
```go
	defer dataobjects.Close()
```
New:
```go
	defer logIfErr(dataobjects.Close, "closing data-objects rows")
```

- [ ] **Step 3: Edit `reindex.go` line 283**

Old:
```go
	defer colls.Close()
```
New:
```go
	defer logIfErr(colls.Close, "closing collections rows")
```

- [ ] **Step 4: Edit `reindex.go` line 410**

Old:
```go
	defer avusRows.Close()
```
New:
```go
	defer logIfErr(avusRows.Close, "closing AVUs rows (deferred)")
```

- [ ] **Step 5: Edit `reindex.go` line 416**

Old:
```go
	avusRows.Close()
```
New:
```go
	logIfErr(avusRows.Close, "closing AVUs rows")
```

(Note: this is the non-deferred close right after `preprocessMetadata`. We still log because we want to know if it failed.)

- [ ] **Step 6: Edit `reindex.go` line 448**

Old:
```go
	defer indexer.Flush()
```
New:
```go
	defer logIfErr(indexer.Flush, "flushing bulk indexer (deferred)")
```

- [ ] **Step 7: Edit `tags.go` line 67**

Old:
```go
	defer tags.Close()
```
New:
```go
	defer logIfErr(tags.Close, "closing tags rows")
```

- [ ] **Step 8: Edit `tags.go` line 140**

Old:
```go
	defer indexer.Flush()
```
New:
```go
	defer logIfErr(indexer.Flush, "flushing tags bulk indexer (deferred)")
```

- [ ] **Step 9: Build, test, lint**

Run:
```bash
go build ./...
go test ./...
golangci-lint run
```
Expected: build and tests pass; **no more errcheck findings** — only the 3 staticcheck (QF1003) warnings remain.

- [ ] **Step 10: Commit**

```bash
git add reindex.go tags.go
git commit -m "use logIfErr for deferred Close/Flush in reindex and tags"
```

### Task 4c: Convert if/else-if chains to switch (staticcheck QF1003)

**Files:**
- Modify: `reindex.go` (lines 247–256, 312–321, 352–361)

The three findings are all `if/else if` chains comparing a single variable. Convert each to a tagged `switch`.

- [ ] **Step 1: Confirm the staticcheck warnings**

Run: `golangci-lint run 2>&1 | grep QF1003`
Expected: 3 findings as above.

- [ ] **Step 2: Refactor `reindex.go` ~line 247 (inside `processDataobjects`)**

Old:
```go
		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.dataobjectsUpdated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.dataobjectsAdded++
		}
```
New:
```go
		switch classification {
		case UpdateDocument:
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.dataobjectsUpdated++
		case IndexDocument:
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.dataobjectsAdded++
		}
```

- [ ] **Step 3: Refactor `reindex.go` ~line 312 (inside `processCollections`)**

Old:
```go
		if classification == UpdateDocument {
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.collsUpdated++
		} else if classification == IndexDocument {
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.collsAdded++
		}
```
New:
```go
		switch classification {
		case UpdateDocument:
			log.Debugf("data-object %s, documents differ, indexing", id)
			rows.collsUpdated++
		case IndexDocument:
			log.Debugf("data-object %s not in ES, indexing", id)
			rows.collsAdded++
		}
```

- [ ] **Step 4: Refactor `reindex.go` ~line 352 (inside `processDeletions`)**

Old:
```go
			if docType == "file" {
				log.Debugf("data-object %s not seen in ICAT, deleting", id)
				rows.dataobjectsRemoved++
			} else if docType == "folder" {
				log.Debugf("collection %s not seen in ICAT, deleting", id)
				rows.collsRemoved++
			}
```
New:
```go
			switch docType {
			case "file":
				log.Debugf("data-object %s not seen in ICAT, deleting", id)
				rows.dataobjectsRemoved++
			case "folder":
				log.Debugf("collection %s not seen in ICAT, deleting", id)
				rows.collsRemoved++
			}
```

- [ ] **Step 5: Build, test, lint**

Run:
```bash
go build ./...
go test ./...
golangci-lint run
```
Expected: build and tests pass; **golangci-lint reports 0 issues**.

- [ ] **Step 6: Commit**

```bash
git add reindex.go
git commit -m "convert if/else-if chains to tagged switch (staticcheck QF1003)"
```

---

## Final verification

- [ ] **Step 1: Full check from a clean state**

Run:
```bash
go mod tidy
go build ./...
go test ./...
golangci-lint run
```
Expected: clean output from every command. `go mod tidy` should produce no diff (if it does, commit the tidy result).

- [ ] **Step 2: Container build sanity check**

Run:
```bash
docker build -t infosquito2:final .
docker run --rm infosquito2:final --help
```
Expected: build succeeds, help text prints.
