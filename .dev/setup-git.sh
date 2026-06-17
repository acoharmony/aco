#!/bin/bash
# Git Setup Script - Automated configuration for this repository
set -e

echo "🔧 Setting up Git configuration..."

# Use SSH for ordinary `git push` / `git pull`. HTTPS token auth has been
# flaky in this environment, while SSH is the path used by the release flow.
git remote set-url origin git@github.com:acoharmony/aco.git
echo "✅ origin configured for SSH"

# Configure git to use shared hooks directory
git config core.hooksPath .githooks
echo "✅ Git hooks configured to use .githooks/"

# Make hooks executable
chmod +x .githooks/*
echo "✅ Hooks made executable"

echo ""
echo "🎉 Git setup complete!"
echo ""
echo "Your workflow:"
echo "  1. Always start from main: git checkout main && git pull"
echo "  2. Create feature branch: git checkout -b feature/name"
echo "  3. Commit freely on feature branch"
echo "  4. Push and create PR: git push -u origin feature/name"
echo "  5. After merge: git checkout main && git pull && git branch -d feature/name"
echo ""
echo "⚠️  Note: Direct commits to 'main' are now blocked by pre-commit hook"
echo ""
echo "Coverage baseline:"
echo "  Coverage accumulates across incremental test runs (--cov-append)."
echo "  To seed a fresh baseline:  uv run coverage erase && .githooks/run-coverage"
echo "  To reset stale data:       uv run coverage erase"
echo ""
echo "Release automation:"
echo "  To run the full post-commit release train after commits on main:"
echo "    git config acoharmony.releaseAfterCommit true"
echo "  To allow main commits without auto-release:"
echo "    git config acoharmony.allowMainCommits true"
echo "  The release hook pushes HEAD, waits for CI, tags the next patch release,"
echo "  waits for the GitHub Release workflow, then builds/pushes GHCR images."
echo "  Manual equivalent:"
echo "    scripts/release_after_ci.sh"
