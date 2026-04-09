#!/usr/bin/env python3
"""
© 2025 HarmonyCares
Automate marimo custom index.html updates

This script updates the custom index.html with the latest asset hashes
from the installed marimo package whenever marimo is upgraded.
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

# ACOharmony customization script to inject
CUSTOMIZATION_SCRIPT = """    <!-- ACOharmony Custom Homepage -->
    <script data-marimo="true">
      // ACOharmony Homepage Override
      // © 2025 HarmonyCares
      (function() {
          'use strict';

          console.log('ACOharmony Homepage Override loading...');

          function hideUnwantedSections() {
              // Hide Resources, Create Notebook, and Tutorials sections using CSS
              const style = document.createElement('style');
              style.textContent = `
                  /* Hide marimo logo in header to avoid duplication */
                  header img[src*="logo"] { display: none !important; }

                  /* Hide Resources section - look for links to Documentation, GitHub, Discord, etc */
                  div.flex.flex-col.gap-2:has(a[href*="marimo.io"]) { display: none !important; }
                  div.flex.flex-col.gap-2:has(a[href*="github.com/marimo"]) { display: none !important; }
                  div.flex.flex-col.gap-2:has(a[href*="discord"]) { display: none !important; }

                  /* Hide Tutorials dropdown */
                  button[data-testid="open-tutorial-button"] { display: none !important; }

                  /* Style workspace headers with HarmonyCares colors */
                  div.flex.flex-col.gap-2 h2 {
                      color: #2E3254 !important;
                      border-bottom: 3px solid #1EDAC9 !important;
                      padding-bottom: 0.5rem !important;
                      margin-bottom: 1.25rem !important;
                  }

                  /* Dashboard tiles styles */
                  .aco-tiles-grid {
                      display: grid;
                      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                      gap: 1.25rem;
                      margin-bottom: 2rem;
                  }

                  .aco-tile {
                      background: white;
                      border-radius: 8px;
                      padding: 1.5rem;
                      box-shadow: 0 2px 8px rgba(46, 50, 84, 0.1);
                      transition: all 0.3s ease;
                      border: 2px solid transparent;
                      text-decoration: none;
                      color: inherit;
                      display: block;
                  }

                  .aco-tile:hover {
                      transform: translateY(-4px);
                      box-shadow: 0 8px 16px rgba(46, 50, 84, 0.15);
                      border-color: #1EDAC9;
                  }

                  .aco-tile-header {
                      display: flex;
                      align-items: center;
                      gap: 1rem;
                      margin-bottom: 0.75rem;
                  }

                  .aco-tile-icon {
                      width: 48px;
                      height: 48px;
                      border-radius: 8px;
                      display: flex;
                      align-items: center;
                      justify-content: center;
                      font-size: 1.5rem;
                      background: linear-gradient(135deg, #2E3254 0%, #434A80 100%);
                      color: #1EDAC9;
                  }

                  .aco-tile-title {
                      font-size: 1.125rem;
                      font-weight: 600;
                      color: #2E3254;
                  }

                  .aco-tile-description {
                      color: #666;
                      font-size: 0.875rem;
                      line-height: 1.5;
                  }
              `;
              document.head.appendChild(style);
          }

          function isHomePage() {
              // Check if we're on the homepage (no file parameter in URL)
              const urlParams = new URLSearchParams(window.location.search);
              return !urlParams.has('file');
          }

          function addCustomHeader() {
              // Only add custom header on homepage
              if (!isHomePage()) {
                  return;
              }

              const root = document.querySelector('#root');
              if (!root) {
                  setTimeout(addCustomHeader, 100);
                  return;
              }

              // Check if already added
              if (document.getElementById('aco-header')) {
                  return;
              }

              console.log('Adding ACOharmony header and dashboard...');

              // Find the main content container - look for the container with max-width class
              const mainContainer = root.querySelector('div.container') ||
                                   root.querySelector('div[class*="max-w"]') ||
                                   root.querySelector('div > div > div[class*="flex flex-col gap-6"]');

              if (!mainContainer) {
                  console.log('Main container not found, retrying...');
                  setTimeout(addCustomHeader, 100);
                  return;
              }

              console.log('Found main container:', mainContainer.className);

              // Create header element
              const header = document.createElement('div');
              header.id = 'aco-header';
              header.style.cssText = 'background: linear-gradient(135deg, #2E3254 0%, #434A80 100%); border-radius: 12px; padding: 2rem; margin-bottom: 2rem; box-shadow: 0 4px 12px rgba(46, 50, 84, 0.15);';

              header.innerHTML = `
                  <div style="display: flex; align-items: center; gap: 1.5rem; color: white;">
                      <img src="logo.png" alt="HarmonyCares Logo" style="height: 60px; width: auto;" onerror="console.error('Logo failed to load')">
                      <div>
                          <h1 style="font-size: 2rem; font-weight: 600; margin: 0; color: white;">ACOharmony</h1>
                          <p style="margin: 0.5rem 0 0 0; color: #1EDAC9; font-size: 1.1rem; font-weight: 500;">Interactive Data Platform</p>
                      </div>
                  </div>
              `;

              // Create dashboard tiles section with flex-col gap-2 wrapper to match Marimo's structure
              const tilesWrapper = document.createElement('div');
              tilesWrapper.className = 'flex flex-col gap-2';
              tilesWrapper.id = 'aco-tiles-wrapper';

              const tilesSection = document.createElement('div');
              tilesSection.id = 'aco-tiles';
              tilesSection.innerHTML = `
                  <h2 style="color: #2E3254; border-bottom: 3px solid #1EDAC9; padding-bottom: 0.5rem; margin-bottom: 1.25rem; font-size: 1.5rem; font-weight: 600;">Quick Access</h2>
                  <div class="aco-tiles-grid">
                      <a href="https://github.com/acoharmony/acoharmony" class="aco-tile" target="_blank">
                          <div class="aco-tile-header">
                              <div class="aco-tile-icon">🔧</div>
                              <div class="aco-tile-title">GitHub</div>
                          </div>
                          <div class="aco-tile-description">Source code management and Git repository</div>
                      </a>
                      <a href="https://github.com/acoharmony?ecosystem=container&tab=packages" class="aco-tile" target="_blank">
                          <div class="aco-tile-header">
                              <div class="aco-tile-icon">📦</div>
                              <div class="aco-tile-title">Container Registry</div>
                          </div>
                          <div class="aco-tile-description">Docker container image registry</div>
                      </a>
                      <a href="http://localhost:10013" class="aco-tile" target="_blank">
                          <div class="aco-tile-header">
                              <div class="aco-tile-icon">📖</div>
                              <div class="aco-tile-title">Docusaurus</div>
                          </div>
                          <div class="aco-tile-description">Project documentation and guides</div>
                      </a>
                      <a href="http://localhost:10012" class="aco-tile" target="_blank">
                          <div class="aco-tile-header">
                              <div class="aco-tile-icon">📓</div>
                              <div class="aco-tile-title">Marimo</div>
                          </div>
                          <div class="aco-tile-description">Interactive Python notebooks and analytics</div>
                      </a>
                  </div>
              `;

              tilesWrapper.appendChild(tilesSection);

              // Create Git Repository section
              const gitWrapper = document.createElement('div');
              gitWrapper.className = 'flex flex-col gap-2';
              gitWrapper.id = 'aco-git-wrapper';
              gitWrapper.style.marginBottom = '2rem';

              const gitSection = document.createElement('div');
              gitSection.id = 'aco-git';
              gitSection.innerHTML = `
                  <h2 style="color: #2E3254; border-bottom: 3px solid #1EDAC9; padding-bottom: 0.5rem; margin-bottom: 1.25rem; font-size: 1.5rem; font-weight: 600;">Git Repository</h2>
                  <div style="background: white; border-radius: 8px; padding: 1.5rem; box-shadow: 0 2px 8px rgba(46, 50, 84, 0.1); border: 2px solid #f0f0f0;">
                      <div style="display: flex; flex-direction: column; gap: 1rem;">
                          <div style="display: flex; align-items: center; gap: 1rem;">
                              <div style="width: 48px; height: 48px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.5rem; background: linear-gradient(135deg, #2E3254 0%, #434A80 100%); color: #1EDAC9;">
                                  🔧
                              </div>
                              <div style="flex: 1;">
                                  <div style="font-size: 1.25rem; font-weight: 600; color: #2E3254; margin-bottom: 0.25rem;">
                                      acoharmony
                                  </div>
                                  <div style="color: #666; font-size: 0.875rem;">
                                      HarmonyCares OS Launch
                                  </div>
                              </div>
                              <a href="https://github.com/acoharmony/acoharmony" target="_blank" style="padding: 0.5rem 1rem; background: linear-gradient(135deg, #2E3254 0%, #434A80 100%); color: white; border-radius: 6px; text-decoration: none; font-weight: 500; transition: all 0.3s ease;">
                                  View on GitHub →
                              </a>
                          </div>
                          <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; padding-top: 1rem; border-top: 1px solid #e0e0e0;">
                              <div>
                                  <div style="color: #666; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.25rem;">Branch</div>
                                  <div style="color: #2E3254; font-weight: 500; font-family: monospace;">main</div>
                              </div>
                              <div>
                                  <div style="color: #666; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.25rem;">Remote</div>
                                  <div style="color: #2E3254; font-weight: 500; font-family: monospace;">origin</div>
                              </div>
                              <div>
                                  <div style="color: #666; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.25rem;">Project</div>
                                  <div style="color: #2E3254; font-weight: 500;">HarmonyCares OS Launch</div>
                              </div>
                          </div>
                      </div>
                  </div>
              `;

              gitWrapper.appendChild(gitSection);

              // Insert header first, then tiles section, then git section
              mainContainer.insertBefore(header, mainContainer.firstChild);
              mainContainer.insertBefore(tilesWrapper, mainContainer.children[1]);
              mainContainer.insertBefore(gitWrapper, mainContainer.children[2]);

              console.log('✓ ACOharmony header, dashboard tiles, and git repository added');
          }

          // Apply styles immediately
          hideUnwantedSections();

          // Add header when ready
          if (document.readyState === 'loading') {
              document.addEventListener('DOMContentLoaded', addCustomHeader);
          } else {
              setTimeout(addCustomHeader, 100);
          }

          // Watch for React re-renders and reapply (only on homepage)
          const observer = new MutationObserver(() => {
              if (isHomePage() && (!document.getElementById('aco-header') || !document.getElementById('aco-tiles-wrapper') || !document.getElementById('aco-git-wrapper'))) {
                  addCustomHeader();
              }
          });

          observer.observe(document.body, {
              childList: true,
              subtree: true
          });

          // Handle URL changes (for client-side navigation)
          let lastUrl = window.location.href;
          const checkUrlChange = () => {
              const currentUrl = window.location.href;
              if (currentUrl !== lastUrl) {
                  lastUrl = currentUrl;
                  console.log('URL changed to:', currentUrl);
                  // Remove custom elements if navigating away from homepage
                  if (!isHomePage()) {
                      console.log('Not on homepage, removing custom elements');
                      const header = document.getElementById('aco-header');
                      const tiles = document.getElementById('aco-tiles-wrapper');
                      const git = document.getElementById('aco-git-wrapper');
                      if (header) header.remove();
                      if (tiles) tiles.remove();
                      if (git) git.remove();
                  } else {
                      // Re-add if navigating back to homepage
                      console.log('On homepage, adding custom elements');
                      addCustomHeader();
                  }
              }
          };

          // Check for URL changes periodically
          setInterval(checkUrlChange, 500);

          // Also listen to popstate events
          window.addEventListener('popstate', checkUrlChange);
      })();
    </script>
"""


def main():
    """Update custom index.html with latest marimo assets."""
    # Paths
    script_dir = Path(__file__).parent
    custom_dir = script_dir / "custom"
    venv_dir = Path(os.environ.get("VENV_DIR", "/home/care/acoharmony/.venv"))
    marimo_static = venv_dir / "lib/python3.13/site-packages/marimo/_static"

    print("\033[1;33mACOharmony Marimo Index Updater\033[0m")
    print("=================================\n")

    # Check if marimo is installed
    if not marimo_static.exists():
        print(f"\033[0;31mError: marimo not found at {marimo_static}\033[0m")
        print("Please install marimo first: uv sync --all-extras")
        return 1

    # Get marimo version
    try:
        import marimo

        marimo_version = marimo.__version__
    except ImportError:
        marimo_version = "unknown"

    print(f"Marimo version: \033[0;32m{marimo_version}\033[0m")

    # Backup existing custom index.html
    custom_index = custom_dir / "index.html"
    if custom_index.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_file = custom_dir / f"index.html.backup-{timestamp}"
        print(f"Backing up existing index.html to: {backup_file}")
        shutil.copy2(custom_index, backup_file)

    # Copy new index.html from marimo package
    print("Copying new index.html from marimo package...")
    marimo_index = marimo_static / "index.html"
    content = marimo_index.read_text()

    # Insert customization script before </body>
    print("Adding ACOharmony customization script...")
    content = content.replace("  </body>", CUSTOMIZATION_SCRIPT + "\n  </body>")

    # Write updated index.html
    custom_index.write_text(content)

    print("\033[0;32m✓ Custom index.html updated successfully!\033[0m\n")
    print("Next steps:")
    print("  1. Restart marimo container: cd deploy/compose && docker compose restart marimo")
    print("  2. Verify at http://localhost:10012")

    return 0


if __name__ == "__main__":
    exit(main())
