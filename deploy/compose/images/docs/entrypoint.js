#!/usr/bin/env node
/**
 * Docusaurus entrypoint for distroless container
 * No bash available - pure Node.js execution
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const DOCS_DIR = '/home/care/acoharmony/docs';

// Check if package.json exists (volume-mounted source)
if (!fs.existsSync(path.join(DOCS_DIR, 'package.json'))) {
  console.error('ERROR: package.json not found in', DOCS_DIR);
  console.error('Ensure the source directory is mounted correctly');
  process.exit(1);
}

// Parse command line arguments
const args = process.argv.slice(2);

// If custom command provided (for builder service), execute it
if (args.length > 0) {
  console.log('Running custom command:', args.join(' '));

  // Handle npm run commands by translating to direct docusaurus calls
  if (args[0] === 'npm' && args[1] === 'run' && args[2] === 'build') {
    console.log('Building Docusaurus site for production...');

    // Build to temp location (Docusaurus needs to own the directory)
    const tempBuildDir = '/tmp/docusaurus-build';
    const finalBuildDir = path.join(DOCS_DIR, 'build');

    const docusaurusCliPath = '/home/care/acoharmony/docs/node_modules/@docusaurus/core/bin/docusaurus.mjs';

    const cmd = spawn('/nodejs/bin/node', [docusaurusCliPath, 'build', '--out-dir', tempBuildDir], {
      cwd: DOCS_DIR,
      stdio: 'inherit',
      env: process.env
    });

    cmd.on('error', (err) => {
      console.error('Failed to build:', err);
      process.exit(1);
    });

    cmd.on('exit', (code) => {
      if (code === 0) {
        console.log('Build successful, copying to volume...');
        // Clean final build directory
        if (fs.existsSync(finalBuildDir)) {
          const files = fs.readdirSync(finalBuildDir);
          for (const file of files) {
            const filePath = path.join(finalBuildDir, file);
            try {
              if (fs.lstatSync(filePath).isDirectory()) {
                fs.rmSync(filePath, { recursive: true, force: true });
              } else {
                fs.unlinkSync(filePath);
              }
            } catch (err) {
              console.warn(`Warning: Could not remove ${filePath}:`, err.message);
            }
          }
        }
        // Copy from temp to volume
        const tempFiles = fs.readdirSync(tempBuildDir);
        for (const file of tempFiles) {
          const src = path.join(tempBuildDir, file);
          const dest = path.join(finalBuildDir, file);
          if (fs.lstatSync(src).isDirectory()) {
            fs.cpSync(src, dest, { recursive: true });
          } else {
            fs.copyFileSync(src, dest);
          }
        }
        console.log('Build output copied to', finalBuildDir);
      }
      process.exit(code || 0);
    });
  } else {
    // For other commands, try to execute directly
    const cmd = spawn(args[0], args.slice(1), {
      cwd: DOCS_DIR,
      stdio: 'inherit',
      env: {
        ...process.env,
        PATH: `/home/care/acoharmony/docs/node_modules/.bin:${process.env.PATH}`
      }
    });

    cmd.on('error', (err) => {
      console.error('Failed to start command:', err);
      process.exit(1);
    });

    cmd.on('exit', (code) => {
      process.exit(code || 0);
    });
  }

} else {
  // Default: Start Docusaurus development server
  console.log('Starting Docusaurus development server...');
  console.log('Documentation will be available at http://localhost:3000');

  // Use direct path to docusaurus CLI script (distroless doesn't have npx or shell for symlinks)
  const docusaurusCliPath = '/home/care/acoharmony/docs/node_modules/@docusaurus/core/bin/docusaurus.mjs';

  const docusaurus = spawn('/nodejs/bin/node', [docusaurusCliPath, 'start', '--host', '0.0.0.0', '--port', '3000', '--no-open'], {
    cwd: DOCS_DIR,
    stdio: 'inherit',
    env: process.env
  });

  docusaurus.on('error', (err) => {
    console.error('Failed to start Docusaurus:', err);
    process.exit(1);
  });

  docusaurus.on('exit', (code) => {
    process.exit(code || 0);
  });
}

// Handle termination signals
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('Received SIGINT, shutting down gracefully...');
  process.exit(0);
});
