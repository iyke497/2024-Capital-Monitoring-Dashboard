// app/static/js/export.js

$(document).ready(function() {
    const modal = $('#exportModal');
    const exportBtn = $('.export-btn');
    const closeBtn = $('.close');
    const exportForm = $('#exportForm');
    const resetBtn = $('#resetFilters');
    
    // Store all MDAs for filtering
    let allMdas = [];
    let selectedMda = '';
    
    // 1. Load filter options on page load
    function loadFilterOptions() {
        // Load from the new /api/export/filters endpoint
        $.get('/api/export/filters', function(response) {
            if (response.success) {
                // Populate ministries
                const ministrySelect = $('#filter-ministry');
                ministrySelect.find('option:not(:first)').remove(); // Clear existing options except "All Ministries"
                
                response.parent_ministries.forEach(ministry => {
                    ministrySelect.append(
                        `<option value="${ministry}">${ministry}</option>`
                    );
                });
                
                // Store MDAs for searchable dropdown
                allMdas = response.mdas || [];
                
                // Populate states
                const stateSelect = $('#filter-state');
                stateSelect.find('option:not(:first)').remove(); // Clear existing options except "All States"
                
                response.states.forEach(state => {
                    stateSelect.append(`<option value="${state}">${state}</option>`);
                });
            }
        }).fail(function(error) {
            console.error('Failed to load filter options:', error);
        });
    }
    
    // 2. Initialize searchable MDA dropdown
    function initSearchableMdaDropdown() {
        const mdaSearchInput = $('#mda-search-input');
        const mdaDropdownList = $('#mda-dropdown-list');
        const mdaSearchContainer = $('.mda-search-container');
        const selectedMdaDisplay = $('#selected-mda-display');
        const clearMdaBtn = $('#clear-mda-selection');
        
        // Show dropdown when input is focused
        mdaSearchInput.on('focus', function() {
            if (allMdas.length > 0) {
                filterAndDisplayMdas('');
                mdaDropdownList.show();
            }
        });
        
        // Filter MDAs as user types
        mdaSearchInput.on('input', function() {
            const searchTerm = $(this).val();
            filterAndDisplayMdas(searchTerm);
        });
        
        // Clear MDA selection
        clearMdaBtn.on('click', function() {
            selectedMda = '';
            mdaSearchInput.val('');
            selectedMdaDisplay.hide();
            mdaSearchContainer.show();
            $('#filter-mda').val(''); // Clear hidden input
        });
        
        // Close dropdown when clicking outside
        $(document).on('click', function(e) {
            if (!$(e.target).closest('.mda-search-wrapper').length) {
                mdaDropdownList.hide();
            }
        });
        
        // Handle keyboard navigation
        mdaSearchInput.on('keydown', function(e) {
            const items = mdaDropdownList.find('.mda-dropdown-item:visible');
            const current = mdaDropdownList.find('.mda-dropdown-item.highlighted');
            
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                if (current.length === 0) {
                    items.first().addClass('highlighted');
                } else {
                    current.removeClass('highlighted');
                    const next = current.next('.mda-dropdown-item:visible');
                    if (next.length) {
                        next.addClass('highlighted');
                    } else {
                        items.first().addClass('highlighted');
                    }
                }
                scrollToHighlighted();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                if (current.length === 0) {
                    items.last().addClass('highlighted');
                } else {
                    current.removeClass('highlighted');
                    const prev = current.prev('.mda-dropdown-item:visible');
                    if (prev.length) {
                        prev.addClass('highlighted');
                    } else {
                        items.last().addClass('highlighted');
                    }
                }
                scrollToHighlighted();
            } else if (e.key === 'Enter') {
                e.preventDefault();
                if (current.length) {
                    current.click();
                }
            } else if (e.key === 'Escape') {
                mdaDropdownList.hide();
            }
        });
    }
    
    // Filter and display MDAs based on search term
    function filterAndDisplayMdas(searchTerm) {
        const mdaDropdownList = $('#mda-dropdown-list');
        mdaDropdownList.empty();
        
        const normalizedSearch = searchTerm.toLowerCase().trim();
        
        // Filter MDAs
        const filteredMdas = allMdas.filter(mda => 
            mda.toLowerCase().includes(normalizedSearch)
        );
        
        // Show message if no results
        if (filteredMdas.length === 0) {
            mdaDropdownList.append(
                '<div class="mda-dropdown-item no-results">No MDAs found</div>'
            );
            mdaDropdownList.show();
            return;
        }
        
        // Limit results to prevent performance issues
        const displayLimit = 50;
        const mdasToDisplay = filteredMdas.slice(0, displayLimit);
        
        // Display filtered MDAs
        mdasToDisplay.forEach(mda => {
            const item = $('<div class="mda-dropdown-item"></div>').text(mda);
            item.on('click', function() {
                selectMda(mda);
            });
            mdaDropdownList.append(item);
        });
        
        // Show "more results" message if needed
        if (filteredMdas.length > displayLimit) {
            mdaDropdownList.append(
                `<div class="mda-dropdown-item more-results">
                    Showing ${displayLimit} of ${filteredMdas.length} results. Keep typing to narrow down...
                </div>`
            );
        }
        
        mdaDropdownList.show();
    }
    
    // Select an MDA
    function selectMda(mda) {
        selectedMda = mda;
        $('#filter-mda').val(mda); // Set hidden input value
        $('#selected-mda-display span').text(mda);
        $('#selected-mda-display').show();
        $('.mda-search-container').hide();
        $('#mda-dropdown-list').hide();
        
        console.log('MDA selected:', mda); // Debug log
        console.log('Hidden input value:', $('#filter-mda').val()); // Debug log
        
        // Trigger change event for preview
        $('#filter-mda').trigger('change');
    }
    
    // Scroll to highlighted item in dropdown
    function scrollToHighlighted() {
        const highlighted = $('#mda-dropdown-list .mda-dropdown-item.highlighted');
        if (highlighted.length) {
            const dropdown = $('#mda-dropdown-list');
            const itemTop = highlighted.position().top;
            const itemBottom = itemTop + highlighted.outerHeight();
            const dropdownHeight = dropdown.height();
            
            if (itemBottom > dropdownHeight) {
                dropdown.scrollTop(dropdown.scrollTop() + itemBottom - dropdownHeight);
            } else if (itemTop < 0) {
                dropdown.scrollTop(dropdown.scrollTop() + itemTop);
            }
        }
    }
    
    // 3. Open modal when export button is clicked
    exportBtn.on('click', function(e) {
        e.preventDefault();
        modal.addClass('show');
        loadFilterOptions(); // Refresh options each time modal opens
    });
    
    // 4. Close modal handlers
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
    
    // 5. Reset filters
    resetBtn.on('click', function() {
        exportForm[0].reset();
        $('#exportPreview').hide();
        
        // Reset MDA selection
        selectedMda = '';
        $('#filter-mda').val(''); // Reset hidden input
        $('#selected-mda-display').hide();
        $('.mda-search-container').show();
        $('#mda-search-input').val('');
        $('#mda-dropdown-list').hide();
    });
    
    // 6. Handle form submission
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
    
    // 7. Show export success message
    function showExportSuccess() {
        const preview = $('#exportPreview');
        preview.html(`
            <p style="font-size: 14px; color: var(--eyemark-green); margin: 0;">
                <strong>✓ Export successful!</strong> Your download should begin shortly.
            </p>
        `).show();
    }
    
    // 8. Preview count when filters change (debounced)
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
        
        // Explicitly include all form fields including hidden input
        for (const [key, value] of formData.entries()) {
            if (value) {
                params.append(key, value);
            }
        }
        
        // Double-check that mda_name is included if selected
        const mdaValue = $('#filter-mda').val();
        if (mdaValue && !params.has('mda_name')) {
            params.append('mda_name', mdaValue);
        }
        
        console.log('Preview count params:', params.toString()); // Debug log
        
        // Call the count endpoint
        $.get(`/api/export/count?${params.toString()}`, function(data) {
            if (data.success) {
                console.log('Preview count result:', data.count); // Debug log
                $('#previewCount').text(data.count);
                $('#exportPreview').html(`
                    <p style="font-size: 14px; color: #666;">
                        <strong>Preview:</strong> <span id="previewCount">${data.count}</span> responses will be exported
                    </p>
                `).show();
            }
        }).fail(function(error) {
            console.error('Preview count failed:', error); // Debug log
            // Silently fail - preview is optional
            $('#exportPreview').hide();
        });
    }
    
    // Initialize searchable dropdown
    initSearchableMdaDropdown();
});
