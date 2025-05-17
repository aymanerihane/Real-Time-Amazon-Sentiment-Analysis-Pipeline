import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS } from 'chart.js';
import { connectWebSocket, addMessageHandler, removeMessageHandler } from '../../utils/dataUtils';

export default function BarChart() {
  const chartRef = useRef(null);          // For canvas element
  const chartInstance = useRef(null);     // For chart instance
  const [reviewData, setReviewData] = useState({});

  useEffect(() => {
    // Connect to WebSocket
    connectWebSocket();

    // Handle incoming messages
    const handleMessage = (message) => {
      if (message.type === 'new_sentiment') {
        setReviewData(prevData => {
          const newData = { ...prevData };
          const asin = message.data.asin;
          
          if (!newData[asin]) {
            newData[asin] = {
              title: message.data.title,
              sentimentCounts: {
                positive: 0,
                neutral: 0,
                negative: 0
              }
            };
          }
          
          newData[asin].sentimentCounts[message.data.sentiment]++;
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
    // Guard clause if canvas isn't ready
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    
    // Clean up previous chart if exists
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }

    // Get top 5 ASINs by total reviews
    const topASINs = Object.entries(reviewData)
      .sort(([, a], [, b]) => {
        const totalA = Object.values(a.sentimentCounts).reduce((sum, count) => sum + count, 0);
        const totalB = Object.values(b.sentimentCounts).reduce((sum, count) => sum + count, 0);
        return totalB - totalA;
      })
      .slice(0, 5);

    // Create new chart
    chartInstance.current = new ChartJS(ctx, {
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

    // Cleanup function
    return () => {
      if (chartInstance.current) {
        chartInstance.current.destroy();
        chartInstance.current = null;
      }
    };
  }, [reviewData]); // Re-render when reviewData changes

  return (
    <div className="chart-container">
      <canvas ref={chartRef} />
    </div>
  );
}