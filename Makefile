# =============================================================================
# Convenience targets mirroring the Examples CI gates (.github/workflows/examples.yml).
#
# These reproduce the COMPILE / TYPECHECK gates locally — they do NOT run the
# examples against a connector (a live run needs a KubeMQ GCP Pub/Sub connector
# on :8085; see the live-run-docs job in the workflow and
# examples/SHARED-CONVENTIONS.md). Toolchains must already be installed; the
# per-language version floors live in each example manifest.
#
#   make build-examples   # compile/typecheck every language (same as CI)
#   make lint             # per-language linters from SHARED-CONVENTIONS
#   make <lang>           # a single language, e.g. `make go`, `make python`
# =============================================================================

EX := examples

.PHONY: build-examples lint \
        go python ruby java csharp javascript \
        lint-go lint-python lint-ruby lint-java lint-csharp lint-javascript

# ---- aggregate -------------------------------------------------------------

build-examples: go python ruby java csharp javascript ## Compile/typecheck all six languages

lint: lint-go lint-python lint-ruby lint-java lint-csharp lint-javascript ## Lint all six languages

# ---- per-language build / typecheck (mirrors CI) ---------------------------

go: ## Go: go build ./... + go vet ./...
	cd $(EX)/go && go build ./... && go vet ./...

python: ## Python: uv sync + byte-compile every example
	cd $(EX)/python && uv sync --frozen && \
		uv run python -m compileall -q -x '(\.venv|\.ruff_cache)' .

ruby: ## Ruby: bundle install + ruby -c on every example
	cd $(EX)/ruby && bundle install --quiet && \
		find . -name '*.rb' -print0 | xargs -0 -n1 ruby -c

java: ## Java: mvn -q -B compile
	cd $(EX)/java && mvn -q -B -ntp compile

csharp: ## C#: dotnet build (Release)
	cd $(EX)/csharp && dotnet build KubeMQ.GcpPubSub.Examples.sln --configuration Release

javascript: ## JS/TS: npm ci + tsc --noEmit
	cd $(EX)/javascript && npm ci && npm run typecheck

# ---- per-language lint (SHARED-CONVENTIONS) --------------------------------

lint-go: ## Go: gofumpt check + golangci-lint
	cd $(EX)/go && gofumpt -l . && golangci-lint run ./...

lint-python: ## Python: ruff format --check + ruff check
	cd $(EX)/python && uv run ruff format --check . && uv run ruff check .

lint-ruby: ## Ruby: rubocop
	cd $(EX)/ruby && bundle exec rubocop

lint-java: ## Java: compile-as-lint (no separate linter)
	cd $(EX)/java && mvn -q -B -ntp compile

lint-csharp: ## C#: dotnet format (verify only)
	cd $(EX)/csharp && dotnet format --verify-no-changes

lint-javascript: ## JS/TS: tsc --noEmit
	cd $(EX)/javascript && npm run typecheck
