// ACOharmony Custom Homepage Redirect
// This script automatically redirects the Marimo homepage to your custom dashboard

(function() {
    'use strict';

    // Configuration
    const CUSTOM_DASHBOARD_PATH = 'dashboard.py'; // Change this to your dashboard file path (relative to /opt/s3/data/notebooks)
    const REDIRECT_DELAY = 100; // milliseconds

    // Check if we're on the home page (not already in a notebook)
    function isHomePage() {
        // Check if the URL doesn't have a 'file' parameter
        const urlParams = new URLSearchParams(window.location.search);
        return !urlParams.has('file') && window.location.pathname === '/';
    }

    // Perform the redirect
    function redirectToDashboard() {
        if (isHomePage()) {
            console.log('ACOharmony: Redirecting to custom dashboard...');
            const dashboardUrl = `/?file=${encodeURIComponent(CUSTOM_DASHBOARD_PATH)}`;

            setTimeout(() => {
                window.location.href = dashboardUrl;
            }, REDIRECT_DELAY);
        }
    }

    // Execute when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', redirectToDashboard);
    } else {
        redirectToDashboard();
    }
})();

