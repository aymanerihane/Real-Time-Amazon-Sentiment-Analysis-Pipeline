import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS, registerables } from 'chart.js';
import { connectWebSocket, addMessageHandler, removeMessageHandler, globalDataStore } from '../../utils/dataUtils';

// Register the chart components
ChartJS.register(...registerables);

export default function BarChart({ filters = {} }) {
  const chartRef = useRef(null);          // For canvas element
  const chartInstance = useRef(null);     // For chart instance
  // Generate a truly unique ID for this chart instance
  const [chartId] = useState(`bar-chart-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`);
  const [reviewData, setReviewData] = useState({});

  // Function to safely destroy chart
  const destroyChart = () => {
    if (chartInstance.current) {
      chartInstance.current.destroy();
      chartInstance.current = null;
    }
  };

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages - just for component updates when data changes
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment' || message.type === 'new_review') {
        // Update chart with filtered data from global store
        applyFiltersToData();
      }
    };

    addMessageHandler(handleMessage);

    // Initial data load from global store
    applyFiltersToData();

    return () => {
      removeMessageHandler(handleMessage);
      // Clean up chart on unmount
      destroyChart();
    };
  }, []);

  // Apply filters when they change
  useEffect(() => {
    applyFiltersToData();
  }, [filters]);

  // Function to apply filters to global data
  const applyFiltersToData = () => {
    let timeRangeFilter;
    const now = Date.now();
    
    // Determine time range filter
    switch (filters.timeRange) {
      case '24h':
        timeRangeFilter = now - 24 * 60 * 60 * 1000;
        break;
      case '7d':
        timeRangeFilter = now - 7 * 24 * 60 * 60 * 1000;
        break;
      case '30d':
        timeRangeFilter = now - 30 * 24 * 60 * 60 * 1000;
        break;
      case '90d':
        timeRangeFilter = now - 90 * 24 * 60 * 60 * 1000;
        break;
      default:
        timeRangeFilter = 0; // All time
    }
    
    // Filter data based on filters
    const filteredData = {};
    Object.entries(globalDataStore.asinReviewData).forEach(([asin, data]) => {
      // Apply ASIN filter if not 'all'
      if (filters.asin !== 'all' && asin !== filters.asin) return;
      
      // Apply time range filter if timestamp is available
      if (data.timestamp && data.timestamp < timeRangeFilter) return;
      
      filteredData[asin] = {
        ...data,
        sentimentCounts: { ...data.sentimentCounts }
      };
      
      // Apply sentiment filter if not 'all'
      if (filters.sentiment !== 'all') {
        const sentiments = ['positive', 'neutral', 'negative'];
        sentiments.forEach(sentiment => {
          if (sentiment !== filters.sentiment) {
            filteredData[asin].sentimentCounts[sentiment] = 0;
          }
        });
      }
    });
    
    setReviewData(filteredData);
  };

  useEffect(() => {
    // First destroy any existing chart
    destroyChart();

    // Guard clause if canvas isn't ready
    if (!chartRef.current) return;

    // Get top 5 ASINs by total reviews
    const topASINs = Object.entries(reviewData)
      .sort(([, a], [, b]) => {
        const totalA = Object.values(a.sentimentCounts).reduce((sum, count) => sum + count, 0);
        const totalB = Object.values(b.sentimentCounts).reduce((sum, count) => sum + count, 0);
        return totalB - totalA;
      })
      .slice(0, 5);

    // Use a longer timeout to ensure DOM is ready and previous chart is fully cleaned up
    const timer = setTimeout(() => {
      try {
        // Check again if the canvas reference is still valid
        if (!chartRef.current) return;
          
        const ctx = chartRef.current.getContext('2d');
        if (!ctx) {
          console.error('Failed to get 2D context for canvas');
          return;
        }
        
        // Create new chart with the unique ID
        chartInstance.current = new ChartJS(ctx, {
          id: chartId,
          type: 'bar',
          data: {
            labels: topASINs.map(([asin]) => asin),
            datasets: [
              {
                label: 'Positive',
                data: topASINs.map(([, data]) => data.sentimentCounts.positive),
                backgroundColor: '#10B981',
                borderRadius: 4
              },
              {
                label: 'Neutral',
                data: topASINs.map(([, data]) => data.sentimentCounts.neutral),
                backgroundColor: '#3B82F6',
                borderRadius: 4
              },
              {
                label: 'Negative',
                data: topASINs.map(([, data]) => data.sentimentCounts.negative),
                backgroundColor: '#EF4444',
                borderRadius: 4
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                stacked: true,
                grid: {
                  display: false
                }
              },
              y: {
                stacked: true,
                beginAtZero: true,
                grid: {
                  drawBorder: false
                }
              }
            },
            plugins: {
              legend: {
                position: 'top',
                labels: {
                  usePointStyle: true,
                  padding: 20
                }
              },
              tooltip: {
                mode: 'index',
                intersect: false,
                callbacks: {
                  title: (items) => {
                    const asin = items[0].label;
                    return reviewData[asin]?.title || asin;
                  }
                }
              }
            }
          }
        });
      } catch (error) {
        console.error("Chart creation error:", error);
      }
    }, 100); // Longer delay to ensure cleanup

    // Cleanup function
    return () => {
      clearTimeout(timer);
      destroyChart();
    };
  }, [reviewData, chartId]); // Re-render when reviewData or chartId changes

  return (
    <div className="chart-container" style={{ height: "300px" }}>
      <canvas id={chartId} ref={chartRef} />
    </div>
  );
}