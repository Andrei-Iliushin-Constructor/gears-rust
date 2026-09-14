---
title: Prepare environment
description: Copy and run the complete prerequisites installation for macOS, Linux, or Windows.
sidebar:
  label: Prepare environment
  order: 2
---

Choose your operating system and run the commands in order. `rustup` installs Rust and Cargo together, then `rustup default stable` selects the Rust toolchain used by this repository.

## macOS

### 1. Install system tools

Run this in Terminal. If `xcode-select --install` opens an installer, complete it, reopen Terminal, and run the remaining commands.

```bash
xcode-select --install

# Install system tools
brew install git make cmake protobuf python pipx

# Install rustup, Rust, and Cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup default stable
rustup component add clippy rustfmt
```

### 2. Optional: install Constructor Studio

Constructor Studio is installed automatically when a target such as `make all` needs it. Run these commands now only if you want to install `cfs` yourself:

```bash
pipx ensurepath
source ~/.zshrc
pipx install git+https://github.com/constructorfabric/studio.git
```

## Linux (Debian/Ubuntu)

### 1. Install system tools

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake protobuf-compiler python3 python3-pip pipx curl git make pkg-config libssl-dev

# Install rustup, Rust, and Cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup default stable
rustup component add clippy rustfmt
```

### 2. Optional: install Constructor Studio

```bash
pipx ensurepath
source ~/.profile
pipx install git+https://github.com/constructorfabric/studio.git
```

## Windows

The supported full workflow uses WSL 2 with Ubuntu because the repository Makefile expects a POSIX shell.

### 1. Install WSL 2

Open PowerShell as Administrator and run:

```powershell
wsl --install -d Ubuntu
```

Restart when prompted, then open the **Ubuntu** application from the Start menu.

### 2. Install system tools in Ubuntu

Copy this block into the Ubuntu terminal:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake protobuf-compiler python3 python3-pip pipx curl git make pkg-config libssl-dev

# Install rustup, Rust, and Cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup default stable
rustup component add clippy rustfmt
```

### 3. Optional: install Constructor Studio in Ubuntu

```bash
pipx ensurepath
source ~/.profile
pipx install git+https://github.com/constructorfabric/studio.git
```

## Next: build and run

After preparing the environment, copy and run the commands in [Build and run](../). That step clones the repository, runs `make setup` to install repository-managed development tools, builds the server, and starts the SQLite quickstart.
