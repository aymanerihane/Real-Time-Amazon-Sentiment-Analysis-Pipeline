import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS } from 'chart.js';
import { connectWebSocket, addMessageHandler, removeMessageHandler } from '../../utils/dataUtils';

export default function LineChart() {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  const [timeSeriesData, setTimeSeriesData] = useState({
    positive: [],
    neutral: [],
    negative: []
  });

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment') {
        const timestamp = new Date(message.data.processed_at).getTime();
        const sentiment = message.data.sentiment;

        setTimeSeriesData(prevData => {
          const newData = {
            positive: [...prevData.positive],
            neutral: [...prevData.neutral],
            negative: [...prevData.negative]
          };

          // Add new data point
          newData[sentiment].push({
            x: timestamp,
            y: 1
          });

          // Keep only last 24 hours of data
          const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
          Object.keys(newData).forEach(key => {
            newData[key] = newData[key].filter(point => point.x >= oneDayAgo);
          });

          return newData;
        });
      }
    };

    addMessageHandler(handleMessage);

    return () => {
      removeMessageHandler(handleMessage);
    };
  }, []);

  useEffect(() => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }

    chartInstance.current = new ChartJS(ctx, {
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

    return () => {
      if (chartInstance.current) {
        chartInstance.current.destroy();
        chartInstance.current = null;
      }
    };
  }, [timeSeriesData]);

  return (
    <div className="chart-container">
      <canvas ref={chartRef} />
    </div>
  );
}