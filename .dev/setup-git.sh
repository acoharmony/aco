#!/bin/bash
# Git Setup Script - Automated configuration for this repository
set -e

echo "🔧 Setting up Git configuration..."

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
