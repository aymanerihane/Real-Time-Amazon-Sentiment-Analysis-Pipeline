import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS, registerables } from 'chart.js';
import { connectWebSocket, addMessageHandler, removeMessageHandler, globalDataStore } from '../../utils/dataUtils';
import 'chartjs-adapter-date-fns'; // Import date adapter

// Register the chart components
ChartJS.register(...registerables);

export default function LineChart({ filters = {} }) {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  // Generate a truly unique ID for this chart instance based on timestamp and random string
  const [chartId] = useState(`line-chart-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`);
  
  // Initialize with filtered data from the global store
  const [timeSeriesData, setTimeSeriesData] = useState(() => {
    return { ...globalDataStore.sentimentTimeSeriesData };
  });
  
  // Function to safely destroy chart
  const destroyChart = () => {
    if (chartInstance.current) {
      try {
        chartInstance.current.destroy();
      } catch (e) {
        console.error("Error destroying chart:", e);
      }
      chartInstance.current = null;
    }
  };

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages - just for component updates when data changes
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment' || message.type === 'new_review') {
        // Use the global data without modifying it
        // This will trigger a re-render when new data arrives
        setTimeSeriesData({ ...globalDataStore.sentimentTimeSeriesData });
      }
    };

    addMessageHandler(handleMessage);

    return () => {
      removeMessageHandler(handleMessage);
      // Clean up the chart on unmount
      destroyChart();
    };
  }, []);

  // Apply filters when they change or when timeSeriesData changes
  useEffect(() => {
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
    
    // Filter data points based on filters
    const filteredData = {
      positive: [],
      neutral: [],
      negative: []
    };

    Object.keys(globalDataStore.sentimentTimeSeriesData).forEach(sentiment => {
      filteredData[sentiment] = globalDataStore.sentimentTimeSeriesData[sentiment].filter(point => {
        // Apply time range filter
        if (point.x < timeRangeFilter) return false;
        
        // Apply ASIN filter if not 'all'
        if (filters.asin !== 'all' && point.asin !== filters.asin) return false;
        
        // Apply sentiment filter if not 'all'
        if (filters.sentiment !== 'all' && sentiment !== filters.sentiment) return false;
        
        return true;
      });
    });
    
    // Update displayed data
    setTimeSeriesData(filteredData);
  }, [filters, globalDataStore.sentimentTimeSeriesData]);

  // Chart rendering effect
  useEffect(() => {
    // First destroy any existing chart
    destroyChart();

    // Return early if canvas is not available
    if (!chartRef.current) return;

    let chartCreationTimeout;
    
    // Use a longer timeout to ensure DOM is ready and previous chart is fully cleaned up
    chartCreationTimeout = setTimeout(() => {
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
          type: 'line',
          data: {
            datasets: [
              {
                label: 'Positive',
                data: timeSeriesData.positive,
                borderColor: '#10B981',
                backgroundColor: '#10B98133',
                fill: true,
                tension: 0.4
              },
              {
                label: 'Neutral',
                data: timeSeriesData.neutral,
                borderColor: '#3B82F6',
                backgroundColor: '#3B82F633',
                fill: true,
                tension: 0.4
              },
              {
                label: 'Negative',
                data: timeSeriesData.negative,
                borderColor: '#EF4444',
                backgroundColor: '#EF444433',
                fill: true,
                tension: 0.4
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                type: 'time',
                time: {
                  unit: 'hour',
                  displayFormats: {
                    hour: 'HH:mm'
                  }
                },
                grid: {
                  display: false
                }
              },
              y: {
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
                intersect: false
              }
            }
          }
        });
      } catch (error) {
        console.error("Chart creation error:", error);
      }
    }, 100); // Longer delay to ensure cleanup

    return () => {
      clearTimeout(chartCreationTimeout);
      destroyChart();
    };
  }, [timeSeriesData, chartId]);

  return (
    <div className="chart-container" style={{ height: "300px" }}>
      <canvas id={chartId} ref={chartRef} />
    </div>
  );
}