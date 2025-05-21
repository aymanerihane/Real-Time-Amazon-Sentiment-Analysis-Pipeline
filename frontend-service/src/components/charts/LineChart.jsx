import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS, registerables } from 'chart.js';
import { connectWebSocket, addMessageHandler, removeMessageHandler } from '../../utils/dataUtils';

// Register the chart components
ChartJS.register(...registerables);

export default function LineChart({ filters = {} }) {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  // Generate a truly unique ID for this chart instance based on timestamp and random string
  const [chartId] = useState(`line-chart-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`);
  
  // Store all data points for filtering
  const [allData, setAllData] = useState({
    positive: [],
    neutral: [], 
    negative: []
  });
  
  // Initialize with some sample data points to ensure the chart displays properly
  const [timeSeriesData, setTimeSeriesData] = useState(() => {
    // Create sample data points for the last 24 hours
    const now = Date.now();
    const initialData = {
      positive: [],
      neutral: [],
      negative: []
    };
    
    // Add sample points every 6 hours for the past 24 hours
    for (let i = 0; i < 4; i++) {
      const timePoint = now - (i * 6 * 60 * 60 * 1000);
      initialData.positive.push({ x: timePoint, y: 0 });
      initialData.neutral.push({ x: timePoint, y: 0 });
      initialData.negative.push({ x: timePoint, y: 0 });
    }
    
    return initialData;
  });

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

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment' || message.type === 'new_review') {
        const data = message.data || message;
        if (!data || !data.sentiment) return;

        // Get timestamp from the data or use current time
        const timestamp = new Date(data.processed_at || data.prediction_time || new Date()).getTime();
        
        // Convert the sentiment to a standardized format (positive, neutral, negative)
        let sentimentCategory;
        if (data.sentiment === 2 || data.sentiment === 'positive' || data.sentiment_label === 'Positive') {
          sentimentCategory = 'positive';
        } else if (data.sentiment === 1 || data.sentiment === 'neutral' || data.sentiment_label === 'Neutral') {
          sentimentCategory = 'neutral';
        } else if (data.sentiment === 0 || data.sentiment === 'negative' || data.sentiment_label === 'Negative') {
          sentimentCategory = 'negative';
        } else {
          console.warn('Unknown sentiment value:', data.sentiment);
          return; // Skip this message as we don't know how to categorize it
        }

        setAllData(prevData => {
          const newData = {
            positive: [...prevData.positive],
            neutral: [...prevData.neutral],
            negative: [...prevData.negative]
          };

          // Add new data point to the appropriate category with ASIN for filtering
          newData[sentimentCategory].push({
            x: timestamp,
            y: 1,
            asin: data.asin || 'unknown'
          });

          return newData;
        });
      }
    };

    addMessageHandler(handleMessage);

    return () => {
      removeMessageHandler(handleMessage);
      // Clean up the chart on unmount
      destroyChart();
    };
  }, []);

  // Apply filters when they change or when allData changes
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

    Object.keys(allData).forEach(sentiment => {
      filteredData[sentiment] = allData[sentiment].filter(point => {
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
  }, [filters, allData]);

  // Chart rendering effect
  useEffect(() => {
    // First destroy any existing chart
    destroyChart();

    // Return early if canvas is not available
    if (!chartRef.current) return;

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
      clearTimeout(timer);
      destroyChart();
    };
  }, [timeSeriesData, chartId]);

  return (
    <div className="chart-container" style={{ height: "300px" }}>
      <canvas id={chartId} ref={chartRef} />
    </div>
  );
}