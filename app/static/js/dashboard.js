// app/static/js/dashboard.js

$(document).ready(function() {
    let countdownInterval;
    let wasFetching = false;
    let bestMinistries = [];
    let worstMinistries = [];
    
    // 1. Initialize DataTables
    const responsesTable = $('#responses-table').DataTable({
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
    
    // Initialize static tables for ministry rankings
    const bestTable = $('#best-ministries-table').DataTable({
        paging: false,
        searching: false,
        info: false,
        order: [[5, 'desc']], // Sort by performance score
        columns: [
            { data: 'ministry_name' },
            { data: 'total_mdas' },
            { data: 'expected_projects' },
            { data: 'reported_projects' },
            { 
                data: 'compliance_rate_pct',
                render: function(data) {
                    return data.toFixed(1) + '%';
                }
            },
            { 
                data: 'performance_index',
                render: function(data) {
                    let badgeClass = 'performance-medium';
                    if (data >= 70) badgeClass = 'performance-high';
                    else if (data < 40) badgeClass = 'performance-low';
                    return `<span class="performance-badge ${badgeClass}">${data.toFixed(1)}</span>`;
                }
            }
        ]
    });
    
    const worstTable = $('#worst-ministries-table').DataTable({
        paging: false,
        searching: false,
        info: false,
        order: [[5, 'asc']], // Sort by performance score (lowest first)
        columns: [
            { data: 'ministry_name' },
            { data: 'total_mdas' },
            { data: 'expected_projects' },
            { data: 'reported_projects' },
            { 
                data: 'compliance_rate_pct',
                render: function(data) {
                    return data.toFixed(1) + '%';
                }
            },
            { 
                data: 'performance_index',
                render: function(data) {
                    let badgeClass = 'performance-medium';
                    if (data >= 70) badgeClass = 'performance-high';
                    else if (data < 40) badgeClass = 'performance-low';
                    return `<span class="performance-badge ${badgeClass}">${data.toFixed(1)}</span>`;
                }
            }
        ]
    });
    
    // 2. Tab switching functionality
    $('.pill-btn').on('click', function() {
        const tabName = $(this).data('tab');
        
        // Update active pill
        $('.pill-btn').removeClass('active');
        $(this).addClass('active');
        
        // Update active tab content
        $('.tab-pane').removeClass('active');
        $(`#${tabName}-tab`).addClass('active');
    });
    
    // 3. Format timestamp for display
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

    // 4. Update countdown display
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

    // 5. Show/hide update status
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

    // 6. Refresh stats
    function updateStats() {
        $.get('/api/stats', function(data) {
            $('#total-responses').text(data.total_responses);
        });
    }

    // 7. Load ministry rankings
    function loadMinistryRankings() {
        $.get('/api/analytics/ministry-rankings', function(response) {
            if (response.success) {
                bestMinistries = response.data.best;
                worstMinistries = response.data.worst;
                
                // Clear and repopulate tables
                bestTable.clear();
                worstTable.clear();
                
                if (bestMinistries.length > 0) {
                    bestTable.rows.add(bestMinistries).draw();
                }
                
                if (worstMinistries.length > 0) {
                    worstTable.rows.add(worstMinistries).draw();
                }
            }
        }).fail(function(error) {
            console.error('Failed to load ministry rankings:', error);
        });
    }
    
    // 8. Load and render weekly activity chart
    // 8. Load and render activity chart with specified timeframe
    function loadActivityChart(days = 7) {
        // Clear existing chart if it exists
        const existingChart = Chart.getChart("activityChart");
        if (existingChart) {
            existingChart.destroy();
        }
        
        // Update active button state
        $('.timeframe-btn').removeClass('active');
        $(`.timeframe-btn[data-days="${days}"]`).addClass('active');
        
        $.get(`/api/analytics/weekly-activity?days=${days}`, function(response) {
            if (response.success) {
                const data = response.data;
                const ctx = document.getElementById('activityChart');
                
                if (ctx) {
                    // Format dates for labels based on timeframe
                    const labels = data.map(d => {
                        const date = new Date(d.date);
                        if (days === 7) {
                            return date.toLocaleDateString('en-US', { weekday: 'short' });
                        } else {
                            // For 30 days, show day of month
                            return date.getDate();
                        }
                    });
                    
                    // Determine chart type based on data density
                    const chartType = days === 7 ? 'line' : 'bar';
                    
                    new Chart(ctx, {
                        type: chartType,
                        data: {
                            labels: labels,
                            datasets: [{
                                label: days === 7 ? 'Daily Responses' : 'Responses',
                                data: data.map(d => d.total),
                                borderColor: '#4baa73',
                                backgroundColor: days === 7 
                                    ? 'rgba(75, 170, 115, 0.1)'
                                    : 'rgba(75, 170, 115, 0.6)',
                                tension: 0.3,
                                fill: days === 7,
                                pointBackgroundColor: '#4baa73',
                                pointBorderColor: '#fff',
                                pointBorderWidth: 2,
                                pointRadius: days === 7 ? 4 : 3,
                                pointHoverRadius: days === 7 ? 6 : 4,
                                borderWidth: days === 7 ? 2 : 0,
                                borderRadius: days === 30 ? 4 : 0
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: {
                                    display: false
                                },
                                tooltip: {
                                    callbacks: {
                                        title: function(context) {
                                            const index = context[0].dataIndex;
                                            const date = new Date(data[index].date);
                                            if (days === 7) {
                                                return date.toLocaleDateString('en-US', { 
                                                    month: 'short', 
                                                    day: 'numeric',
                                                    year: 'numeric'
                                                });
                                            } else {
                                                return date.toLocaleDateString('en-US', { 
                                                    weekday: 'short',
                                                    month: 'short', 
                                                    day: 'numeric'
                                                });
                                            }
                                        },
                                        label: function(context) {
                                            return `Responses: ${context.parsed.y}`;
                                        }
                                    }
                                }
                            },
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    ticks: {
                                        stepSize: Math.max(1, Math.ceil(Math.max(...data.map(d => d.total)) / 5)),
                                        font: {
                                            size: 11
                                        }
                                    },
                                    grid: {
                                        color: 'rgba(0, 0, 0, 0.05)'
                                    },
                                    title: {
                                        display: true,
                                        text: 'Number of Responses'
                                    }
                                },
                                x: {
                                    ticks: {
                                        font: {
                                            size: 11
                                        },
                                        maxRotation: days === 30 ? 45 : 0
                                    },
                                    grid: {
                                        display: false
                                    }
                                }
                            }
                        }
                    });
                    
                    // Update chart title
                    //$('.stats-card h3').text(`Response Activity (${days} Days)`);
                }
            }
        }).fail(function(error) {
            console.error('Failed to load activity data:', error);
        });
    }

    // 9. Load and render budget reporting pie chart
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

    // 10. Check server status and update UI
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
                responsesTable.ajax.reload();
                updateStats();
                loadActivityChart();
                loadBudgetChart();
                loadMinistryRankings();
            }
            
            wasFetching = data.is_fetching;
            
        }).fail(function() {
            $('#last-updated').text('Last updated: Unable to check');
            $('#next-update').text('Next update: Unable to check');
        });
    }

    // 11. Initial load
    updateStats();
    loadActivityChart(7);
    loadBudgetChart();
    loadMinistryRankings();
    checkStatus();

    // 12. Poll server status every 10 seconds
    setInterval(checkStatus, 10000);

    // 13. Timeframe toggle functionality
    $('.timeframe-btn').on('click', function() {
        const days = parseInt($(this).data('days'));
        loadActivityChart(days);
    });
});