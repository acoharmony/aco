// ACOharmony Marimo UI Customization
// © 2025 HarmonyCares
// This script removes Marimo boilerplate and adds HarmonyCares dashboard tiles

(function() {
    'use strict';

    console.log('ACOharmony UI Customization loading...');

    // Wait for DOM to be ready
    function initCustomUI() {
        // Remove "Create a new notebook" section
        const createNotebookLinks = document.querySelectorAll('a[href][target="_blank"]');
        createNotebookLinks.forEach(link => {
            const text = link.textContent;
            if (text.includes('Create a new notebook') || link.querySelector('h2')?.textContent.includes('Create a new notebook')) {
                link.style.display = 'none';
            }
        });

        // Remove Resources section (Documentation, GitHub, Community, YouTube, Changelog)
        const allSections = document.querySelectorAll('div.flex.flex-col.gap-2');
        allSections.forEach(section => {
            const header = section.querySelector('h2');
            if (header && header.textContent.includes('Resources')) {
                section.style.display = 'none';
            }
        });

        // Remove tutorial button from header
        const tutorialButton = document.querySelector('[data-testid="open-tutorial-button"]');
        if (tutorialButton) {
            tutorialButton.style.display = 'none';
        }

        // Replace page title
        const pageTitle = document.querySelector('h1.page-title');
        if (pageTitle && !pageTitle.textContent.includes('ACOharmony')) {
            pageTitle.textContent = 'ACOharmony Data Platform';
        }

        const pageSubtitle = document.querySelector('p.page-subtitle');
        if (pageSubtitle) {
            pageSubtitle.textContent = 'Interactive Python Notebooks & Analysis';
        }

        // Add HarmonyCares header if not exists
        addCustomHeader();

        // Style existing sections with HarmonyCares branding
        styleWorkspaceSection();

        console.log('✓ ACOharmony UI customization applied');
    }

    function addCustomHeader() {
        // Check if custom header already exists
        if (document.getElementById('aco-custom-header')) return;

        // Find the main container
        const mainContainer = document.querySelector('div[class*="flex flex-col gap-6"]');
        if (!mainContainer) return;

        // Create HarmonyCares header
        const header = document.createElement('div');
        header.id = 'aco-custom-header';
        header.style.cssText = `
            background: linear-gradient(135deg, #2E3254 0%, #434A80 100%);
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            box-shadow: 0 4px 12px rgba(46, 50, 84, 0.15);
        `;

        header.innerHTML = `
            <div style="display: flex; align-items: center; gap: 1.5rem; color: white;">
                <img src="logo.png" alt="HarmonyCares Logo" style="height: 60px; width: auto;">
                <div>
                    <h1 style="font-size: 2rem; font-weight: 600; margin: 0; color: white;">ACOharmony</h1>
                    <p style="margin: 0.5rem 0 0 0; color: #1EDAC9; font-size: 1.1rem; font-weight: 500;">
                        Interactive Data Platform
                    </p>
                </div>
            </div>
        `;

        // Insert at the top of the main container
        mainContainer.insertBefore(header, mainContainer.firstChild);
    }

    function styleWorkspaceSection() {
        // Style "Workspace" section header
        const headers = document.querySelectorAll('h2');
        headers.forEach(header => {
            if (header.textContent.includes('Workspace') ||
                header.textContent.includes('Running notebooks') ||
                header.textContent.includes('Recent notebooks')) {
                header.style.color = '#2E3254';
                header.style.borderBottom = '3px solid #1EDAC9';
                header.style.paddingBottom = '0.5rem';
                header.style.marginBottom = '1.25rem';
                header.style.display = 'inline-block';
            }
        });

        // Style notebook file links
        const fileLinks = document.querySelectorAll('a[href*="?file="]');
        fileLinks.forEach(link => {
            link.style.borderLeft = '3px solid #1EDAC9';
            link.style.transition = 'all 0.3s ease';

            link.addEventListener('mouseenter', () => {
                link.style.backgroundColor = 'rgba(30, 218, 201, 0.05)';
                link.style.borderLeftColor = '#2E3254';
            });

            link.addEventListener('mouseleave', () => {
                link.style.backgroundColor = '';
                link.style.borderLeftColor = '#1EDAC9';
            });
        });
    }

    // Run initialization when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCustomUI);
    } else {
        initCustomUI();
    }

    // Re-run when content changes (for React re-renders)
    const observer = new MutationObserver((mutations) => {
        // Debounce the calls
        clearTimeout(window.acoCustomUITimeout);
        window.acoCustomUITimeout = setTimeout(initCustomUI, 100);
    });

    observer.observe(document.body, {
        childList: true,
        subtree: true
    });

    console.log('ACOharmony UI Customization loaded');
})();
