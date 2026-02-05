// Global variables
let mdaTable;
let ministryTable;

// Tab functionality - Updated for pill buttons
function showTab(tabId) {
    // Hide all tab panes
    $('.tab-pane').removeClass('active').hide();
    
    // Show selected tab pane
    $(`#${tabId}`).addClass('active').show();
    
    // Remove active class from all pill buttons
    $('.pill-btn').removeClass('active');
    
    // Add active class to clicked button
    event.target.classList.add('active');
}

// Modal functionality
function openModal(mdaName, agencyCode, expected, reported, complianceRate) {
    // Set modal title and summary
    $('#modal-title').text(`Project Details: ${mdaName}`);
    $('#modal-mda-name').text(mdaName);
    $('#modal-expected').text(expected);
    $('#modal-reported').text(reported);
    $('#modal-compliance').html(getComplianceHTML(complianceRate));
    
    // Show modal
    $('#project-modal').addClass('show').css('display', 'flex');
    $('body').addClass('modal-open');
    
    // Load project details
    loadMdaProjects(agencyCode);
}

function closeModal() {
    $('#project-modal').removeClass('show').css('display', 'none');
    $('body').removeClass('modal-open');
    $('#project-details-table tbody').empty();
}

// Load project details for a specific MDA
function loadMdaProjects(agencyCode) {
    // Clear existing rows
    $('#project-details-table tbody').empty();
    
    // Show loading state
    $('#project-details-table tbody').html(
        '<tr><td colspan="4" style="text-align: center; padding: 40px;">Loading project details...</td></tr>'
    );
    
    // Make API call to get project details
    $.ajax({
        url: `/api/compliance/mda/${encodeURIComponent(agencyCode)}/projects`,
        method: 'GET',
        success: function(response) {
            if (response.success && response.data) {
                populateProjectTable(response.data);
            } else {
                $('#project-details-table tbody').html(
                    '<tr><td colspan="4" style="text-align: center; padding: 40px; color: #d32f2f;">Failed to load project details</td></tr>'
                );
            }
        },
        error: function() {
            $('#project-details-table tbody').html(
                '<tr><td colspan="4" style="text-align: center; padding: 40px; color: #d32f2f;">Error loading project details</td></tr>'
            );
        }
    });
}

// Populate project table with data
function populateProjectTable(projects) {
    const tbody = $('#project-details-table tbody');
    tbody.empty();
    
    if (projects.length === 0) {
        tbody.html(
            '<tr><td colspan="4" style="text-align: center; padding: 40px;">No project data available</td></tr>'
        );
        return;
    }
    
    projects.forEach(project => {
        const statusClass = project.reported ? 'status-reported' : 'status-not-reported';
        const statusText = project.reported ? 'Reported ✓' : 'Not Reported ✗';
        const budgetFormatted = formatCurrency(project.budget_allocation || 0);
        
        const row = `
            <tr>
                <td><code>${project.project_code || 'N/A'}</code></td>
                <td>${project.project_title || 'Untitled Project'}</td>
                <td style="text-align: right; font-family: monospace;">${budgetFormatted}</td>
                <td><span class="${statusClass}">${statusText}</span></td>
            </tr>
        `;
        tbody.append(row);
    });
}

// Helper function to format currency
function formatCurrency(amount) {
    return '₦' + parseFloat(amount).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

// Helper function to get compliance HTML with color coding
function getComplianceHTML(rate) {
    const color = rate >= 80 ? '#2e7d32' : (rate >= 50 ? '#f57c00' : '#d32f2f');
    return `<strong style="color: ${color}">${rate.toFixed(2)}%</strong>`;
}

// Reset MDA filter
function resetMdaFilter() {
    mdaTable.column(1).search('').draw();
    $('#mda-tab .card h3').html('Project Compliance per MDA');
}

// Document ready
$(document).ready(function() {
    // 1. Initialize MDA Table
    mdaTable = $('#mda-table').DataTable({
        ajax: { 
            url: '/api/compliance/mda', 
            dataSrc: 'data' 
        },
        columns: [
            { 
                data: 'mda_name',
                render: function(data, type, row) {
                    return `<span class="clickable-mda" onclick="openModal('${data.replace(/'/g, "\\'")}', '${row.agency_code}', ${row.expected_projects}, ${row.reported_projects}, ${row.compliance_rate_pct})">${data}</span>`;
                }
            },
            { 
                data: 'parent_ministry', 
                visible: false 
            },
            { 
                data: 'expected_projects', 
                className: 'dt-body-center' 
            },
            { 
                data: 'reported_projects', 
                className: 'dt-body-center' 
            },
            { 
                data: 'total_submissions', 
                className: 'dt-body-center' 
            },
            { 
                data: 'compliance_rate_pct', 
                render: function(data) {
                    return getComplianceHTML(data);
                },
                className: 'dt-body-center'
            }
        ],
        order: [[5, 'desc']], // Sort by compliance rate descending
        pageLength: 25,
        responsive: true
    });

    // 2. Initialize Ministry Table
    ministryTable = $('#ministry-table').DataTable({
        ajax: { 
            url: '/api/compliance/ministry', 
            dataSrc: 'data' 
        },
        columns: [
            { 
                data: 'ministry_name', 
                className: 'clickable-min' 
            },
            { 
                data: 'mda_count',
                className: 'dt-body-center'
            },
            { 
                data: 'total_responses',
                className: 'dt-body-center'
            },
            { 
                data: 'total_budget', 
                render: $.fn.dataTable.render.number(',', '.', 2, '₦'),
                className: 'dt-body-right'
            },
            { 
                data: 'avg_completion', 
                render: d => `<strong>${d}%</strong>`,
                className: 'dt-body-center'
            }
        ],
        order: [[4, 'desc']], // Sort by avg completion descending
        pageLength: 25,
        responsive: true
    });

    // 3. Drill-Down Handler for Ministry Table
    $('#ministry-table tbody').on('click', 'tr', function() {
        const rowData = ministryTable.row(this).data();
        if (!rowData) return;

        const minName = rowData.ministry_name;

        // Filter the MDA table by the hidden parent_ministry column
        mdaTable.column(1).search(minName).draw();

        // Switch to MDA tab
        $('.tab-pane').removeClass('active').hide();
        $('#mda-tab').addClass('active').show();
        $('.pill-btn').removeClass('active');
        $('.pill-btn').first().addClass('active');

        // Update heading with filter
        $('#mda-tab .card h3').html(
            `Agencies under: ${minName} <button class="btn btn-secondary" onclick="resetMdaFilter()" style="margin-left: 10px; font-size: 12px; padding: 6px 12px;">Clear Filter</button>`
        );
    });

    // 4. Close modal when clicking X
    $('.close').on('click', closeModal);
    
    // 5. Close modal when clicking outside
    $(window).on('click', function(event) {
        const modal = $('#project-modal');
        if ($(event.target).hasClass('modal')) {
            closeModal();
        }
    });
    
    // 6. Close modal with Escape key
    $(document).on('keydown', function(event) {
        if (event.key === 'Escape' || event.keyCode === 27) {
            closeModal();
        }
    });
    
    // 7. Initialize pill button click handlers
    $('.pill-btn').on('click', function(e) {
        const targetTab = $(this).attr('onclick').match(/'([^']+)'/)[1];
        
        // Remove active from all
        $('.pill-btn').removeClass('active');
        $('.tab-pane').removeClass('active').hide();
        
        // Add active to clicked
        $(this).addClass('active');
        $(`#${targetTab}`).addClass('active').show();
        
        e.preventDefault();
    });
    
    // Expose functions to global scope
    window.showTab = showTab;
    window.openModal = openModal;
    window.closeModal = closeModal;
    window.resetMdaFilter = resetMdaFilter;
});