# Contributing

## Development tasks

Development tasks are defined in the [`justfile`](justfile) and run with
[just](https://just.systems) (e.g. `brew install just`). Run `just` to list all
recipes; common ones are `just build`, `just test-unit`, `just lint`, and
`just compose-up [scenario]`. `just check` reports which optional tools are installed.

## Commit messages and PR titles

This project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
PRs are squash-merged, so the **PR title** is what lands in `main`'s history — it
must follow the format:

```
<type>[optional scope]: <description>
```

Common types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`.
Use `!` after the type (e.g. `feat!:`) or a `BREAKING CHANGE:` footer for breaking
changes. A CI check ([pr-title.yml](.github/workflows/pr-title.yml)) enforces this
on every PR. See [AGENTS.md](AGENTS.md) for the full type list and rationale.

Individual commits within a PR don't need to follow the format, since they get
squashed — but it doesn't hurt.

## Releases

Releases are automated with [release-please](https://github.com/googleapis/release-please)
and are driven entirely by Conventional Commit PR titles merged to `main` — there
is no manual version bumping.

1. As `feat:`/`fix:`/etc. PRs merge to `main`, the
   [release-please workflow](.github/workflows/release-please.yml) keeps an
   up-to-date "release PR" open. That PR bumps the `VERSION` file and updates
   `CHANGELOG.md` to reflect everything merged since the last release.
2. When you're ready to cut a release, review and merge the release PR.
3. Merging it makes release-please create a matching `vX.Y.Z` git tag and GitHub
   Release.
4. That tag push triggers
   [container-and-platforms.yml](.github/workflows/container-and-platforms.yml),
   which builds and publishes the multi-arch container image tagged with the new
   version.

Do not hand-edit `VERSION` or create `vX.Y.Z` tags yourself — it will conflict
with what release-please tracks in `.release-please-manifest.json`.

## Dependency updates

[Dependabot](.github/dependabot.yml) opens weekly grouped PRs for Go modules
(`fix(deps)`, so they ship in the next patch release) and GitHub Actions
(`ci(deps)`), plus monthly PRs for container base images (`build(deps)`).
OpenTelemetry modules are grouped into a single PR because they must be upgraded
together. Minor and patch updates are queued for auto-merge by
[dependabot-automerge.yml](.github/workflows/dependabot-automerge.yml) once
required checks pass; major updates need a manual review.

[govulncheck](.github/workflows/vulncheck.yml) runs on PRs that touch Go code and
on a twice-weekly schedule. Dependabot does not bump the `toolchain` line in
`go.mod`, so Go standard-library vulnerabilities flagged by the scheduled run
must be fixed by hand:

```sh
go get toolchain@go1.26.N   # latest patch release
just vulncheck
```

To update everything locally and verify the result, run `just deps-update`.
