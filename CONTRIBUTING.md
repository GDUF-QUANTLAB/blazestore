# Contributing to BlazeStore

Thanks for contributing to BlazeStore.

## Development Setup

```bash
git clone https://github.com/GDUF-QUANTLAB/blazestore.git
cd blazestore
uv sync
```

## Development Workflow

1. Create a feature branch from `main`.
2. Make focused changes with tests.
3. Run the full local checks before pushing.
4. Push your branch and open a pull request.

Example:

```bash
git switch -c feat/your-change
uv run ruff check .
uv run ruff format .
uv run pytest
uv build
git add .
git commit -m "Describe your change"
git push -u origin feat/your-change
```

## Pull Request Guidelines

- Keep each PR focused on one problem.
- Include or update tests when behavior changes.
- Update `README.md` when public usage changes.
- Ensure CI is green (`ruff check`, `ruff format --check`, `pytest`, `build`).

## Commit Message Guidance

Use clear, imperative commit messages, for example:

- `Harden local store path validation`
- `Add SQL table resolution tests`
- `Clarify README install instructions`
