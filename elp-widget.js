(function() {
  'use strict';
  
  // Configuration
  const CONFIG = {
    dataUrl: 'https://bkwiseman-del.github.io/elp-dashboard-data/elp_data.json',
    dashboardUrl: 'https://www.trucksafe.com/elp-dashboard',
    containerId: 'trucksafe-elp-widget',
    refreshInterval: 3600000 // 1 hour in milliseconds
  };
  
  // Widget HTML template
  function createWidgetHTML(data) {
    // Calculate MoM trend
    const monthlyLabels = data.monthly.labels;
    const monthlyOOS = data.monthly.oos;
    let momTrend = 0;
    let momLabel = '';
    let trendColor = '#6b7280';
    let trendArrow = '→';
    
    if (monthlyLabels.length >= 3) {
      const lastFullMonth = monthlyOOS[monthlyOOS.length - 2];
      const prevFullMonth = monthlyOOS[monthlyOOS.length - 3];
      const lastMonthLabel = monthlyLabels[monthlyLabels.length - 2];
      const prevMonthLabel = monthlyLabels[monthlyLabels.length - 3];
      
      if (prevFullMonth > 0) {
        momTrend = ((lastFullMonth - prevFullMonth) / prevFullMonth * 100);
        momLabel = prevMonthLabel + ' vs ' + lastMonthLabel;
        
        if (momTrend < 0) {
          trendColor = '#10b981';
          trendArrow = '↘';
        } else if (momTrend > 0) {
          trendColor = '#ef4444';
          trendArrow = '↗';
        }
      }
    }
    
    // Get top 3 states
    const topStates = data.states.filter(s => s.state.toUpperCase() !== 'US').slice(0, 3);
    const maxOOS = topStates[0]?.oos || 1;
    
    return `
      <div style="width: 450px; max-width: 100%; border: 2px solid #d1d5db; border-radius: 16px; padding: 0; background: white; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; box-shadow: 0 4px 6px rgba(0,0,0,0.1); overflow: hidden;">
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #4b5563 0%, #374151 100%); color: white; padding: 20px;">
          <div style="font-size: 20px; font-weight: bold; margin-bottom: 4px;">ELP Enforcement Tracker</div>
          <div style="font-size: 13px; color: #d1d5db;">Real-time FMCSA violation data</div>
        </div>
        
        <!-- Content -->
        <div style="padding: 20px;">
          <!-- Main Stat -->
          <div style="background: #f9fafb; border-radius: 12px; padding: 16px; margin-bottom: 16px; border: 1px solid #e5e7eb;">
            <div style="font-size: 12px; color: #6b7280; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Total OOS Violations Since Jun '25</div>
            <div style="font-size: 40px; font-weight: bold; color: #374151; line-height: 1;">${data.total_oos.toLocaleString()}</div>
          </div>
          
          <!-- Stats Grid -->
          <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 16px;">
            <div style="background: #f9fafb; border-radius: 8px; padding: 12px; border: 1px solid #e5e7eb;">
              <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">Peak Month</div>
              <div style="font-size: 22px; font-weight: bold; color: #374151;">${data.peak_month}</div>
              <div style="font-size: 11px; color: #6b7280; margin-top: 2px;">${data.peak_count.toLocaleString()} violations</div>
            </div>
            
            <div style="background: #f9fafb; border-radius: 8px; padding: 12px; border: 1px solid #e5e7eb;">
              <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">Month-over-Month</div>
              <div style="font-size: 22px; font-weight: bold; color: ${trendColor};">${trendArrow} ${momTrend >= 0 ? '+' : ''}${momTrend.toFixed(1)}%</div>
              <div style="font-size: 11px; color: #6b7280; margin-top: 2px;">${momLabel}</div>
            </div>
          </div>
          
          <!-- Top 3 States -->
          <div style="margin-bottom: 16px;">
            <div style="font-size: 12px; font-weight: 600; color: #374151; margin-bottom: 10px;">Top 3 States by OOS Violations</div>
            ${topStates.map((state, idx) => {
              const percentage = (state.oos / maxOOS * 100).toFixed(0);
              const barColor = idx === 0 ? '#6b7280' : idx === 1 ? '#9ca3af' : '#d1d5db';
              return `
                <div style="margin-bottom: 8px;">
                  <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
                    <div style="font-size: 13px; font-weight: 600; color: #1f2937;">${state.state}</div>
                    <div style="font-size: 13px; font-weight: 600; color: #6b7280;">${state.oos.toLocaleString()}</div>
                  </div>
                  <div style="background: #e5e7eb; height: 8px; border-radius: 4px; overflow: hidden;">
                    <div style="background: ${barColor}; height: 100%; width: ${percentage}%; border-radius: 4px;"></div>
                  </div>
                </div>
              `;
            }).join('')}
          </div>
          
          <!-- CTA Button -->
          <a href="${CONFIG.dashboardUrl}" target="_blank" rel="noopener" style="display: block; background: #4b5563; color: white; padding: 12px; border-radius: 8px; text-align: center; text-decoration: none; font-size: 14px; font-weight: 600; margin-bottom: 12px;">
            View Full Interactive Dashboard →
          </a>
          
          <!-- Footer -->
          <div style="text-align: center; font-size: 11px; color: #9ca3af; padding-top: 12px; border-top: 1px solid #e5e7eb;">
            Powered by <strong style="color: #4b5563;">Trucksafe</strong> • Updated daily
          </div>
        </div>
      </div>
    `;
  }
  
  // Fetch data and render widget
  async function loadWidget() {
    try {
      const container = document.getElementById(CONFIG.containerId);
      if (!container) {
        console.error('Trucksafe ELP Widget: Container element not found');
        return;
      }
      
      // Show loading state
      container.innerHTML = '<div style="padding: 20px; text-align: center; color: #6b7280;">Loading ELP data...</div>';
      
      // Fetch data
      const response = await fetch(CONFIG.dataUrl);
      if (!response.ok) {
        throw new Error('Failed to load data');
      }
      
      const data = await response.json();
      
      // Render widget
      container.innerHTML = createWidgetHTML(data);
      
      // Set up auto-refresh
      setTimeout(loadWidget, CONFIG.refreshInterval);
      
    } catch (error) {
      console.error('Trucksafe ELP Widget Error:', error);
      const container = document.getElementById(CONFIG.containerId);
      if (container) {
        container.innerHTML = '<div style="padding: 20px; text-align: center; color: #ef4444;">Failed to load ELP data. Please try again later.</div>';
      }
    }
  }
  
  // Initialize widget when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadWidget);
  } else {
    loadWidget();
  }
  
})();
