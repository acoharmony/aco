// ACOharmony Homepage Override
// © 2025 HarmonyCares
// This script completely replaces the Marimo homepage with HarmonyCares branding

(function() {
    'use strict';

    console.log('ACOharmony Homepage Override loading...');

    // Wait for React to render, then replace content
    function replaceHomepage() {
        // Find the main container
        const container = document.querySelector('div[class*="flex flex-col gap-6"]') ||
                         document.querySelector('#root > div > div > div');

        if (!container) {
            setTimeout(replaceHomepage, 100);
            return;
        }

        // Check if already replaced
        if (document.getElementById('aco-dashboard')) {
            return;
        }

        console.log('Replacing Marimo homepage with ACOharmony dashboard...');

        // Create completely new homepage content
        container.innerHTML = `
            <div id="aco-dashboard" style="max-width: 1400px; margin: 0 auto; padding: 2rem; width: 100%;">
                <!-- HarmonyCares Header -->
                <div style="background: linear-gradient(135deg, #2E3254 0%, #434A80 100%); border-radius: 12px; padding: 2rem; margin-bottom: 2rem; box-shadow: 0 4px 12px rgba(46, 50, 84, 0.15);">
                    <div style="display: flex; align-items: center; gap: 1.5rem; color: white;">
                        <img src="logo.png" alt="HarmonyCares Logo" style="height: 60px; width: auto;">
                        <div>
                            <h1 style="font-size: 2rem; font-weight: 600; margin: 0; color: white;">ACOharmony</h1>
                            <p style="margin: 0.5rem 0 0 0; color: #1EDAC9; font-size: 1.1rem; font-weight: 500;">Interactive Data Platform</p>
                        </div>
                    </div>
                </div>

                <!-- Workspace Section (preserve original) -->
                <div id="workspace-section"></div>
            </div>
        `;

        // Move workspace files section into our custom layout
        setTimeout(() => {
            const workspaceDiv = document.getElementById('workspace-section');
            const originalWorkspace = document.querySelector('div:has(> h2)');

            if (workspaceDiv && originalWorkspace) {
                // Find all workspace-related sections
                const sections = Array.from(document.querySelectorAll('div.flex.flex-col.gap-2'));
                sections.forEach(section => {
                    const header = section.querySelector('h2');
                    if (header && (
                        header.textContent.includes('Workspace') ||
                        header.textContent.includes('Running') ||
                        header.textContent.includes('Recent')
                    )) {
                        // Style the header with HarmonyCares colors
                        header.style.color = '#2E3254';
                        header.style.borderBottom = '3px solid #1EDAC9';
                        header.style.paddingBottom = '0.5rem';
                        header.style.marginBottom = '1.25rem';

                        workspaceDiv.appendChild(section);
                    }
                });
            }
        }, 200);

        console.log('✓ ACOharmony homepage loaded');
    }

    // Start replacement when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', replaceHomepage);
    } else {
        replaceHomepage();
    }

    // Watch for React re-renders and reapply
    const observer = new MutationObserver(() => {
        if (!document.getElementById('aco-dashboard')) {
            replaceHomepage();
        }
    });

    observer.observe(document.body, {
        childList: true,
        subtree: true
    });

})();
