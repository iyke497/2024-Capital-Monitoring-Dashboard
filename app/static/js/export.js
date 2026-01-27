// app/static/js/export.js

$(document).ready(function() {
    const modal = $('#exportModal');
    const exportBtn = $('.export-btn');
    const closeBtn = $('.close');
    const exportForm = $('#exportForm');
    const resetBtn = $('#resetFilters');
    
    // 1. Load filter options on page load
    function loadFilterOptions() {
        // Load ministries
        $.get('/api/analytics/ministry-rankings', function(response) {
            if (response.success) {
                const ministries = new Set();
                response.data.best.forEach(m => ministries.add(m.ministry_name));
                response.data.worst.forEach(m => ministries.add(m.ministry_name));
                
                const sortedMinistries = Array.from(ministries).sort();
                sortedMinistries.forEach(ministry => {
                    $('#filter-ministry').append(
                        `<option value="${ministry}">${ministry}</option>`
                    );
                });
            }
        });
        
        // Load states (Nigerian states)
        const nigerianStates = [
            'Abia', 'Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue',
            'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 'Gombe',
            'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Kogi', 'Kwara',
            'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 'Plateau',
            'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara', 'FCT'
        ];
        
        nigerianStates.forEach(state => {
            $('#filter-state').append(`<option value="${state}">${state}</option>`);
        });
        
        // Load survey types from responses
        $.get('/api/stats', function(data) {
            if (data.survey_types) {
                data.survey_types.forEach(type => {
                    $('#filter-survey-type').append(
                        `<option value="${type}">${type}</option>`
                    );
                });
            }
        });
    }
    
    // 2. Open modal when export button is clicked
    exportBtn.on('click', function(e) {
        e.preventDefault();
        modal.addClass('show');
        loadFilterOptions(); // Refresh options each time modal opens
    });
    
    // 3. Close modal handlers
    closeBtn.on('click', function() {
        modal.removeClass('show');
    });
    
    $(window).on('click', function(e) {
        if ($(e.target).is('#exportModal')) {
            modal.removeClass('show');
        }
    });
    
    // Close on ESC key
    $(document).on('keydown', function(e) {
        if (e.key === 'Escape' && modal.hasClass('show')) {
            modal.removeClass('show');
        }
    });
    
    // 4. Reset filters
    resetBtn.on('click', function() {
        exportForm[0].reset();
        $('#exportPreview').hide();
    });
    
    // 5. Handle form submission
    exportForm.on('submit', function(e) {
        e.preventDefault();
        
        // Show loading state
        const btnText = $('#exportBtnText');
        const btnSpinner = $('#exportBtnSpinner');
        const submitBtn = exportForm.find('button[type="submit"]');
        
        btnText.hide();
        btnSpinner.show();
        submitBtn.prop('disabled', true);
        
        // Build query parameters
        const formData = new FormData(exportForm[0]);
        const params = new URLSearchParams();
        
        for (const [key, value] of formData.entries()) {
            if (value) { // Only add non-empty values
                params.append(key, value);
            }
        }
        
        // Build export URL
        const exportUrl = `/api/export/responses?${params.toString()}`;
        
        // Trigger download by creating a temporary link
        const link = document.createElement('a');
        link.href = exportUrl;
        link.download = `survey_responses_${new Date().toISOString().split('T')[0]}.xlsx`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        // Reset button state after a delay
        setTimeout(function() {
            btnText.show();
            btnSpinner.hide();
            submitBtn.prop('disabled', false);
            
            // Show success message
            showExportSuccess();
            
            // Close modal after 1.5 seconds
            setTimeout(function() {
                modal.removeClass('show');
            }, 1500);
        }, 1000);
    });
    
    // 6. Show export success message
    function showExportSuccess() {
        const preview = $('#exportPreview');
        preview.html(`
            <p style="font-size: 14px; color: var(--eyemark-green); margin: 0;">
                <strong>✓ Export successful!</strong> Your download should begin shortly.
            </p>
        `).show();
    }
    
    // 7. Optional: Preview count when filters change
    let previewTimeout;
    exportForm.find('select, input').on('change', function() {
        clearTimeout(previewTimeout);
        
        // Debounce the preview request
        previewTimeout = setTimeout(function() {
            updatePreviewCount();
        }, 500);
    });
    
    function updatePreviewCount() {
        const formData = new FormData(exportForm[0]);
        const params = new URLSearchParams();
        
        for (const [key, value] of formData.entries()) {
            if (value) {
                params.append(key, value);
            }
        }
        
        // You can create an endpoint that returns just the count
        // For now, we'll skip this feature or implement it later
        // $.get(`/api/export/count?${params.toString()}`, function(data) {
        //     $('#previewCount').text(data.count);
        //     $('#exportPreview').show();
        // });
    }
});