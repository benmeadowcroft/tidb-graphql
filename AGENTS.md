# Agent Instructions

Guidance for any coding agent (Claude, Codex, etc.) working in this repository.

## Commit messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
Releases and the `CHANGELOG.md` are generated automatically from commit history via
[release-please](https://github.com/googleapis/release-please), so commit types
directly control version bumps:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

- `feat:` — a new feature (triggers a **minor** version bump)
- `fix:` — a bug fix (triggers a **patch** version bump)
- `docs:` — documentation only changes
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `perf:` — a code change that improves performance
- `test:` — adding or correcting tests
- `build:` — changes to the build system or dependencies
- `ci:` — changes to CI configuration/workflows
- `chore:` — other changes that don't modify src or test files

A breaking change is indicated with a `!` after the type/scope (e.g. `feat!:`) or a
`BREAKING CHANGE:` footer, and triggers a **major** version bump.

Since PRs in this repo are squash-merged, the **PR title** must itself be a valid
Conventional Commit — it becomes the commit message on `main` and is what
release-please reads. This is enforced by CI (`.github/workflows/pr-title.yml`).

## Releases

Do not hand-edit the `VERSION` file or create release tags manually. release-please
opens and maintains a "release PR" that bumps `VERSION` and updates
`CHANGELOG.md` based on merged commits; merging that PR creates the GitHub
Release and tag, which in turn triggers the container build/publish workflow.
See [CONTRIBUTING.md](CONTRIBUTING.md#releases) for the full process.
