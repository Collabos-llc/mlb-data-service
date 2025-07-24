/**
 * MLB Data Service Monitoring Dashboard
 * Real-time status updates and interactive controls
 */

class MonitoringDashboard {
    constructor() {
        this.updateInterval = 30000; // 30 seconds
        this.updateTimer = null;
        this.alerts = [];
        this.isLoading = false;
        
        this.init();
    }

    init() {
        this.bindEvents();
        this.startAutoRefresh();
        this.loadInitialData();
    }

    bindEvents() {
        // Manual refresh button
        document.getElementById('manual-refresh').addEventListener('click', () => {
            this.refreshData();
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'F5' || (e.ctrlKey && e.key === 'r')) {
                e.preventDefault();
                this.refreshData();
            }
        });

        // Page visibility change handling
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.stopAutoRefresh();
            } else {
                this.startAutoRefresh();
                this.refreshData();
            }
        });
    }

    async loadInitialData() {
        this.showLoading(true);
        await this.refreshData();
        this.showLoading(false);
    }

    startAutoRefresh() {
        if (this.updateTimer) {
            clearInterval(this.updateTimer);
        }
        
        this.updateTimer = setInterval(() => {
            if (!document.hidden && !this.isLoading) {
                this.refreshData();
            }
        }, this.updateInterval);
    }

    stopAutoRefresh() {
        if (this.updateTimer) {
            clearInterval(this.updateTimer);
            this.updateTimer = null;
        }
    }

    showLoading(show) {
        const overlay = document.getElementById('loading-overlay');
        if (show) {
            overlay.classList.add('show');
            this.isLoading = true;
        } else {
            overlay.classList.remove('show');
            this.isLoading = false;
        }
    }

    async refreshData() {
        try {
            // Update refresh button to show loading
            const refreshBtn = document.getElementById('manual-refresh');
            const originalText = refreshBtn.innerHTML;
            refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';
            refreshBtn.disabled = true;

            // Fetch monitoring data
            const response = await fetch('/api/v1/monitoring/status');
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            
            // Update dashboard with new data
            this.updateSystemHealth(data.system_health);
            this.updateDataSources(data.data_sources);
            this.updatePerformanceMetrics(data.performance);
            this.processAlerts(data.alerts || []);
            
            // Update timestamp
            this.updateTimestamp();

            // Restore refresh button
            refreshBtn.innerHTML = originalText;
            refreshBtn.disabled = false;

        } catch (error) {
            console.error('Failed to refresh monitoring data:', error);
            this.addAlert('error', 'Data Refresh Failed', `Unable to update monitoring data: ${error.message}`);
            
            // Show error state
            this.showErrorState();
        }
    }

    updateSystemHealth(healthData) {
        if (!healthData) return;

        // Overall system status
        const statusIndicator = document.getElementById('overall-status');
        const statusLight = statusIndicator.querySelector('.status-light');
        const statusText = statusIndicator.querySelector('.status-text');

        statusLight.className = `status-light ${healthData.status}`;
        statusText.textContent = this.capitalizeFirst(healthData.status);
        statusText.className = `status-text ${healthData.status}`;

        // Individual metrics
        this.updateElement('service-status', healthData.service_status || 'Unknown');
        this.updateElement('database-status', healthData.database_status || 'Unknown');
        this.updateElement('uptime', this.formatUptime(healthData.uptime || 0));
    }

    updateDataSources(dataSources) {
        if (!dataSources) return;

        // FanGraphs Batting
        if (dataSources.fangraphs_batting) {
            this.updateSourceCard('fangraphs-batting', dataSources.fangraphs_batting);
            this.updateElement('fg-batting-count', this.formatNumber(dataSources.fangraphs_batting.total_records || 0));
            this.updateElement('fg-batting-last', this.formatDateTime(dataSources.fangraphs_batting.last_update));
        }

        // FanGraphs Pitching
        if (dataSources.fangraphs_pitching) {
            this.updateSourceCard('fangraphs-pitching', dataSources.fangraphs_pitching);
            this.updateElement('fg-pitching-count', this.formatNumber(dataSources.fangraphs_pitching.total_records || 0));
            this.updateElement('fg-pitching-last', this.formatDateTime(dataSources.fangraphs_pitching.last_update));
        }

        // Statcast
        if (dataSources.statcast) {
            this.updateSourceCard('statcast', dataSources.statcast);
            this.updateElement('statcast-count', this.formatNumber(dataSources.statcast.total_records || 0));
            this.updateElement('statcast-last', this.formatDateTime(dataSources.statcast.last_update));
            this.updateElement('statcast-range', this.formatDateRange(dataSources.statcast.date_range));
        }

        // API Performance
        if (dataSources.api_performance) {
            this.updateSourceCard('api-performance', dataSources.api_performance);
            this.updateElement('api-response-time', `${dataSources.api_performance.avg_response_time || 0}ms`);
            this.updateElement('api-success-rate', `${dataSources.api_performance.success_rate || 0}%`);
        }
    }

    updateSourceCard(cardId, sourceData) {
        const card = document.getElementById(cardId);
        if (!card) return;

        const statusIndicator = card.querySelector('.status-indicator');
        const statusLight = statusIndicator.querySelector('.status-light');
        const statusText = statusIndicator.querySelector('.status-text');

        const status = sourceData.status || 'unknown';
        statusLight.className = `status-light ${status}`;
        statusText.textContent = this.capitalizeFirst(status);
        statusText.className = `status-text ${status}`;
    }

    updatePerformanceMetrics(performance) {
        if (!performance) return;

        // Data trends
        this.updateElement('daily-collections', performance.daily_collections || '--');
        this.updateElement('weekly-growth', performance.weekly_growth || '--');
        this.updateElement('data-freshness', this.formatDataFreshness(performance.data_freshness));

        // System resources
        this.updateResourceBar('memory-usage', 'memory-percent', performance.memory_usage || 0);
        this.updateResourceBar('cpu-usage', 'cpu-percent', performance.cpu_usage || 0);
        this.updateResourceBar('disk-usage', 'disk-percent', performance.disk_usage || 0);
    }

    updateResourceBar(barId, percentId, value) {
        const bar = document.getElementById(barId);
        const percent = document.getElementById(percentId);
        
        if (bar && percent) {
            bar.style.width = `${value}%`;
            percent.textContent = `${value}%`;

            // Update color based on usage
            bar.className = 'resource-fill';
            if (value > 80) {
                bar.classList.add('danger');
            } else if (value > 60) {
                bar.classList.add('warning');
            }
        }
    }

    processAlerts(alerts) {
        // Update internal alerts array
        this.alerts = alerts;
        
        // Update alert display
        this.updateAlertDisplay();
    }

    updateAlertDisplay() {
        const container = document.getElementById('alert-container');
        
        if (this.alerts.length === 0) {
            container.innerHTML = `
                <div class="no-alerts">
                    <i class="fas fa-check-circle"></i>
                    <p>No active alerts</p>
                </div>
            `;
            return;
        }

        container.innerHTML = this.alerts.map(alert => `
            <div class="alert-item ${alert.type}">
                <div class="alert-icon">
                    <i class="fas ${this.getAlertIcon(alert.type)}"></i>
                </div>
                <div class="alert-content">
                    <div class="alert-title">${alert.title}</div>
                    <div class="alert-message">${alert.message}</div>
                    <div class="alert-time">${this.formatDateTime(alert.timestamp)}</div>
                </div>
            </div>
        `).join('');
    }

    addAlert(type, title, message) {
        const alert = {
            type,
            title,
            message,
            timestamp: new Date().toISOString(),
            id: Date.now()
        };
        
        this.alerts.unshift(alert);
        
        // Limit to 10 alerts
        if (this.alerts.length > 10) {
            this.alerts = this.alerts.slice(0, 10);
        }
        
        this.updateAlertDisplay();
    }

    getAlertIcon(type) {
        const icons = {
            error: 'fa-exclamation-circle',
            warning: 'fa-exclamation-triangle',
            info: 'fa-info-circle',
            success: 'fa-check-circle'
        };
        return icons[type] || 'fa-info-circle';
    }

    updateTimestamp() {
        const timestampElement = document.getElementById('last-updated');
        if (timestampElement) {
            timestampElement.textContent = new Date().toLocaleTimeString();
        }
    }

    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        }
    }

    showErrorState() {
        // Update system status to error
        const statusIndicator = document.getElementById('overall-status');
        const statusLight = statusIndicator.querySelector('.status-light');
        const statusText = statusIndicator.querySelector('.status-text');

        statusLight.className = 'status-light error';
        statusText.textContent = 'Error';
        statusText.className = 'status-text error';
    }

    // Utility formatting methods
    capitalizeFirst(str) {
        return str.charAt(0).toUpperCase() + str.slice(1);
    }

    formatNumber(num) {
        if (num === undefined || num === null) return '--';
        return new Intl.NumberFormat().format(num);
    }

    formatDateTime(dateString) {
        if (!dateString) return '--';
        try {
            const date = new Date(dateString);
            return date.toLocaleString();
        } catch {
            return '--';
        }
    }

    formatUptime(seconds) {
        if (!seconds) return '--';
        
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        
        if (days > 0) {
            return `${days}d ${hours}h ${minutes}m`;
        } else if (hours > 0) {
            return `${hours}h ${minutes}m`;
        } else {
            return `${minutes}m`;
        }
    }

    formatDateRange(range) {
        if (!range || !range.start || !range.end) return '--';
        
        try {
            const start = new Date(range.start).toLocaleDateString();
            const end = new Date(range.end).toLocaleDateString();
            return `${start} - ${end}`;
        } catch {
            return '--';
        }
    }

    formatDataFreshness(minutes) {
        if (minutes === undefined || minutes === null) return '--';
        
        if (minutes < 60) {
            return `${minutes} minutes ago`;
        } else if (minutes < 1440) {
            const hours = Math.floor(minutes / 60);
            return `${hours} hour${hours !== 1 ? 's' : ''} ago`;
        } else {
            const days = Math.floor(minutes / 1440);
            return `${days} day${days !== 1 ? 's' : ''} ago`;
        }
    }
}

// Collection and testing functions
async function triggerCollection(source) {
    const modal = document.getElementById('collection-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');
    
    const sourceNames = {
        'fangraphs-batting': 'FanGraphs Batting Data',
        'fangraphs-pitching': 'FanGraphs Pitching Data',
        'statcast': 'Statcast Data'
    };
    
    modalTitle.textContent = `Collect ${sourceNames[source]}`;
    modalBody.innerHTML = '<p><i class="fas fa-spinner fa-spin"></i> Starting data collection...</p>';
    modal.classList.add('show');
    
    try {
        let endpoint;
        let payload = {};
        
        switch (source) {
            case 'fangraphs-batting':
                endpoint = '/api/v1/collect/fangraphs/batting';
                payload = { season: new Date().getFullYear(), min_pa: 10 };
                break;
            case 'fangraphs-pitching':
                endpoint = '/api/v1/collect/fangraphs/pitching';
                payload = { season: new Date().getFullYear(), min_ip: 5 };
                break;
            case 'statcast':
                endpoint = '/api/v1/collect/statcast';
                const today = new Date().toISOString().split('T')[0];
                payload = { start_date: today, end_date: today };
                break;
            default:
                throw new Error('Unknown data source');
        }
        
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            modalBody.innerHTML = `
                <div style="color: var(--success-green);">
                    <i class="fas fa-check-circle"></i>
                    <strong>Collection Successful!</strong>
                </div>
                <p>${result.message}</p>
                <p><strong>Records collected:</strong> ${result.records_collected || 0}</p>
            `;
            
            // Refresh dashboard data
            setTimeout(() => {
                window.dashboard.refreshData();
            }, 2000);
        } else {
            modalBody.innerHTML = `
                <div style="color: var(--danger-red);">
                    <i class="fas fa-exclamation-circle"></i>
                    <strong>Collection Failed!</strong>
                </div>
                <p>${result.message || 'Unknown error occurred'}</p>
            `;
        }
        
    } catch (error) {
        modalBody.innerHTML = `
            <div style="color: var(--danger-red);">
                <i class="fas fa-exclamation-circle"></i>
                <strong>Error!</strong>
            </div>
            <p>Failed to collect data: ${error.message}</p>
        `;
    }
}

async function testApiEndpoints() {
    const modal = document.getElementById('collection-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');
    
    modalTitle.textContent = 'API Endpoint Testing';
    modalBody.innerHTML = '<p><i class="fas fa-spinner fa-spin"></i> Testing API endpoints...</p>';
    modal.classList.add('show');
    
    const endpoints = [
        { name: 'Health Check', url: '/health' },
        { name: 'Service Status', url: '/api/v1/status' },
        { name: 'Analytics Summary', url: '/api/v1/analytics/summary' },
        { name: 'FanGraphs Batting', url: '/api/v1/fangraphs/batting?limit=1' },
        { name: 'Statcast Data', url: '/api/v1/statcast?limit=1' }
    ];
    
    const results = [];
    
    for (const endpoint of endpoints) {
        try {
            const start = Date.now();
            const response = await fetch(endpoint.url);
            const duration = Date.now() - start;
            
            results.push({
                name: endpoint.name,
                status: response.ok ? 'success' : 'error',
                responseTime: duration,
                statusCode: response.status
            });
        } catch (error) {
            results.push({
                name: endpoint.name,
                status: 'error',
                responseTime: 0,
                error: error.message
            });
        }
    }
    
    const successCount = results.filter(r => r.status === 'success').length;
    const avgResponseTime = results.reduce((sum, r) => sum + r.responseTime, 0) / results.length;
    
    modalBody.innerHTML = `
        <div class="test-summary">
            <h4>Test Results</h4>
            <p><strong>Success Rate:</strong> ${successCount}/${results.length} (${Math.round(successCount/results.length*100)}%)</p>
            <p><strong>Average Response Time:</strong> ${Math.round(avgResponseTime)}ms</p>
        </div>
        <div class="test-details">
            ${results.map(result => `
                <div class="test-result ${result.status}">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span>${result.name}</span>
                        <span>${result.status === 'success' ? 
                            `<span style="color: var(--success-green);">✓ ${result.responseTime}ms</span>` : 
                            `<span style="color: var(--danger-red);">✗ Error</span>`
                        }</span>
                    </div>
                    ${result.error ? `<div style="color: var(--danger-red); font-size: 0.8rem;">${result.error}</div>` : ''}
                </div>
            `).join('')}
        </div>
    `;
}

function clearAlerts() {
    if (window.dashboard) {
        window.dashboard.alerts = [];
        window.dashboard.updateAlertDisplay();
    }
}

function closeModal() {
    const modal = document.getElementById('collection-modal');
    modal.classList.remove('show');
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new MonitoringDashboard();
});

// Handle modal close on outside click
document.addEventListener('click', (e) => {
    const modal = document.getElementById('collection-modal');
    if (e.target === modal) {
        closeModal();
    }
});

// Handle modal close on escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});