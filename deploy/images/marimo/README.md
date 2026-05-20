# Marimo Customization Configuration

This directory contains custom branding files for the Marimo service using HarmonyCares/ACOharmony styling.

## Files

```
deploy/conf/marimo/
├── marimo.toml                      # Marimo runtime configuration
├── custom/
│   ├── logo.png                     # HarmonyCares logo
│   └── index.html                   # Customized index with ACOharmony branding
├── update-custom-index.py           # Automation script to update index.html
├── update-custom-index.sh           # Bash wrapper for Python script
└── README.md                         # This file
```

**Note:** The `custom/index.html` must be updated after each marimo upgrade to match new asset hashes.

## Branding

**HarmonyCares Color Palette:**
- Primary: `#2E3254` (Navy)
- Accent: `#1EDAC9` (Teal)
- Secondary: `#434A80` (Purple-gray)
- Tertiary: `#E39B7A` (Coral)

**Logo:** https://harmonycaresaco.com/img/logo.svg

## Problem

Marimo generates unique hash-based filenames for all JavaScript/CSS assets during each build (e.g., `index-DyRdaW9o.js`). When marimo is upgraded, these hashes change, but our custom `index.html` (which is bind-mounted into the container) still references the old hashes, causing the page to fail to render.

## Solution: Automated Update Script

After upgrading marimo, run the automation script to update the custom `index.html`:

```bash
# From the project root
./deploy/conf/marimo/update-custom-index.py

# Or using the bash wrapper
./deploy/conf/marimo/update-custom-index.sh
```

This script:
1. Backs up the existing custom `index.html`
2. Copies the new `index.html` from the marimo package
3. Injects the ACOharmony customization script
4. Saves the updated file

Then restart the marimo container:

```bash
cd deploy/compose
docker compose restart marimo
```

## Docker Bind Mounts

The following bind mounts are configured in `deploy/compose/services/marimo.yml`:

```yaml
volumes:
  # HarmonyCares branding customization
  - /home/care/acoharmony/deploy/conf/marimo/custom/logo.png:/home/care/acoharmony/.venv/lib/python3.13/site-packages/marimo/_static/logo.png:ro
  - /home/care/acoharmony/deploy/conf/marimo/custom/index.html:/home/care/acoharmony/.venv/lib/python3.13/site-packages/marimo/_static/index.html:ro
```

These mounts replace:
1. **Logo** - Marimo's default logo with HarmonyCares logo
2. **Index.html** - Marimo's index with custom version including ACOharmony branding

## Customization Script Features

The customization script injected into `index.html` provides:

- **Custom Header**: ACOharmony branding with HarmonyCares logo
- **Quick Access Tiles**: Links to GitHub, Container Registry, Docusaurus, and Marimo
- **Git Repository Section**: Current repository information
- **Hide Default Sections**: Removes marimo branding, tutorials, and resource links
- **Homepage Detection**: Only shows customizations on the homepage (not in notebooks)
- **Responsive Design**: Auto-fills tiles based on screen width with hover effects

## Manual Process (if script fails)

If you need to manually update the index.html:

1. Find the marimo package location:
   ```bash
   python -c "import marimo; import os; print(os.path.dirname(marimo.__file__))"
   ```

2. Copy the new index.html:
   ```bash
   cp .venv/lib/python3.13/site-packages/marimo/_static/index.html \
      deploy/conf/marimo/custom/index.html
   ```

3. Edit `deploy/conf/marimo/custom/index.html` and add the ACOharmony customization script before `</body>`:
   - The script is located in `update-custom-index.py` as `CUSTOMIZATION_SCRIPT`
   - Insert it between the mount config script and the closing `</body>` tag

4. Restart marimo container

## Testing

1. **Branding appears:** Check homepage shows ACOharmony header and tiles
2. **Logo visible:** HarmonyCares logo displays in header
3. **Navigation works:** Clicking tiles opens correct URLs
4. **Notebooks work:** Opening notebooks should NOT show customizations
5. **File browser works:** Notebooks directory shows all .py files

## Troubleshooting

**Page not rendering:**
- Check browser console for 404 errors on asset files
- Asset hashes in `index.html` may be outdated - run the update script
- Verify bind mount in `deploy/compose/services/marimo.yml` is correct

**Logo not displaying:**
- Verify `custom/logo.png` exists and has correct permissions
- Check bind mount for logo in marimo service configuration
- Browser console should show error if file can't load

**Customizations not appearing:**
- Check browser console for JavaScript errors
- Verify the customization script is present in `index.html`
- Clear browser cache and reload

**Script fails:**
- Ensure marimo is installed: `uv sync --all-extras`
- Check Python version (requires 3.13)
- Verify paths in the script match your environment

## Future Improvements

Potential enhancements to avoid manual intervention:

1. **uv Hook**: Add a post-install hook to run the script automatically after `uv sync`
2. **Docker Build**: Include the script in the Dockerfile build process
3. **Version Check**: Auto-detect marimo version changes and trigger update
4. **Marimo Plugin**: Use marimo's plugin/theming system if they add official customization support

## References

- HarmonyCares website: https://harmonycaresaco.com
- Logo source: https://harmonycaresaco.com/img/logo.svg
- Marimo docs: https://docs.marimo.io
