// app/static/js/dashboard.js

$(document).ready(function() {
    let countdownInterval;
    let wasFetching = false;
    
    // 1. Initialize DataTable
    const table = $('#responses-table').DataTable({
        serverSide: true,
        ajax: '/api/responses',
        columns: [
            { data: 'project_name', defaultContent: 'N/A' },
            { data: 'ergp_code', defaultContent: 'N/A' },
            { data: 'mda_name', defaultContent: 'N/A' },
            { data: 'percentage_completed', defaultContent: '0%' },
            { 
                data: 'project_appropriation_2024',
                render: $.fn.dataTable.render.number(',', '.', 2, '₦')
            }
        ]
    });

    // 2. Format timestamp for display
    function formatTimestamp(isoString) {
        const date = new Date(isoString);
        const timeString = date.toLocaleTimeString('en-US', { 
            hour: '2-digit', 
            minute: '2-digit',
            hour12: true 
        });
        const dateString = date.toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric'
        });
        return `${dateString} at ${timeString}`;
    }

    // 3. Update countdown display
    function updateCountdown(nextRunTime) {
        clearInterval(countdownInterval);
        
        if (!nextRunTime) {
            $('#next-update').text('Next update: Scheduled');
            return;
        }
        
        const nextRun = new Date(nextRunTime);
        
        countdownInterval = setInterval(function() {
            const now = new Date();
            const remaining = nextRun - now;
            
            if (remaining <= 0) {
                $('#next-update').text('Next update: Updating now...');
                setTimeout(checkStatus, 2000);
                return;
            }
            
            const totalMinutes = Math.floor(remaining / 60000);
            const hours = Math.floor(totalMinutes / 60);
            const minutes = totalMinutes % 60;
            const seconds = Math.floor((remaining % 60000) / 1000);
            
            if (hours > 0) {
                $('#next-update').text(`Next update in: ${hours}h ${minutes}m`);
            } else if (minutes > 0) {
                $('#next-update').text(`Next update in: ${minutes}m ${seconds}s`);
            } else {
                $('#next-update').text(`Next update in: ${seconds}s`);
            }
        }, 1000);
    }

    // 4. Show/hide update status
    function showUpdateStatus(isUpdating) {
        const statusDiv = $('#update-status');
        const icon = $('#status-icon');
        const text = $('#status-text');
        
        if (isUpdating) {
            icon.text('⏳');
            text.text('Server is fetching latest data...');
            statusDiv.css('background', '#fff3cd').show();
        } else {
            statusDiv.fadeOut();
        }
    }

    // 5. Refresh stats
    function updateStats() {
        $.get('/api/stats', function(data) {
            $('#total-responses').text(data.total_responses);
            $('#survey1-count').text(data.survey1_count);
            $('#survey2-count').text(data.survey2_count);
        });
    }

    // 6. Load and render budget reporting pie chart
    function loadBudgetChart() {
        $.get('/api/analytics/budget-reporting', function(response) {
            if (response.success) {
                const data = response.data;
                const ctx = document.getElementById('budgetChart');
                
                if (ctx) {
                    new Chart(ctx, {
                        type: 'doughnut',
                        data: {
                            labels: ['Reported Projects', 'Unreported Projects'],
                            datasets: [{
                                data: [data.reported_projects, data.unreported_projects],
                                backgroundColor: ['#4baa73', '#e0e0e0'],
                                borderColor: ['#4baa73', '#e0e0e0'],
                                borderWidth: 2
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: {
                                    position: 'bottom',
                                    labels: {
                                        padding: 15,
                                        font: {
                                            size: 12
                                        }
                                    }
                                },
                                tooltip: {
                                    callbacks: {
                                        label: function(context) {
                                            const label = context.label || '';
                                            const value = context.parsed || 0;
                                            const total = data.total_budget_projects;
                                            const percentage = ((value / total) * 100).toFixed(1);
                                            return `${label}: ${value.toLocaleString()} (${percentage}%)`;
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }).fail(function(error) {
            console.error('Failed to load budget reporting data:', error);
        });
    }

    // 7. Check server status and update UI
    function checkStatus() {
        $.get('/api/fetch/status', function(data) {
            if (data.last_fetch) {
                $('#last-updated').text('Last updated: ' + formatTimestamp(data.last_fetch));
            } else {
                $('#last-updated').text('Last updated: Never');
            }
            
            if (data.next_scheduled_run) {
                updateCountdown(data.next_scheduled_run);
            }
            
            showUpdateStatus(data.is_fetching);
            
            if (wasFetching && !data.is_fetching) {
                console.log('Fetch completed, reloading data...');
                table.ajax.reload();
                updateStats();
                loadBudgetChart();
            }
            
            wasFetching = data.is_fetching;
            
        }).fail(function() {
            $('#last-updated').text('Last updated: Unable to check');
            $('#next-update').text('Next update: Unable to check');
        });
    }

    // 8. Initial load
    updateStats();
    loadBudgetChart();
    checkStatus();

    // 9. Poll server status every 10 seconds
    setInterval(checkStatus, 10000);
});