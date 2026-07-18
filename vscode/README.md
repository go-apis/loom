# Loom Schema Language for VS Code

Syntax highlighting, comment toggling, and bracket support for `.loom`
files — the Loom event-sourcing schema language (see `DESIGN.md`).

## Install (from this repo)

VS Code picks up any extension folder placed in its extensions directory:

```sh
# WSL / remote server
ln -s "$(pwd)/vscode" ~/.vscode-server/extensions/inflow.loom-lang-0.1.0

# local (Linux/mac)
ln -s "$(pwd)/vscode" ~/.vscode/extensions/inflow.loom-lang-0.1.0
```

then reload the window. Or package it properly:

```sh
npx @vscode/vsce package          # in loom/vscode
code --install-extension loom-lang-0.1.0.vsix
```

## Roadmap

A language server (diagnostics via `sdl.Parse`, go-to-definition for type
and event refs, hover with payloads) is planned alongside the Loom console
milestone.
