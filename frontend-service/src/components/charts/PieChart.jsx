import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS } from 'chart.js';

export default function PieChart({ positiveCount, neutralCount, negativeCount }) {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);
  const [chartId] = useState(`chart-${Math.random().toString(36).substring(2, 9)}`);

  useEffect(() => {
    // Ensure the canvas element exists
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    
    // Destroy previous chart if it exists
    if (chartInstance.current) {
      chartInstance.current.destroy();
      chartInstance.current = null;
    }

    // Small delay to ensure the previous chart is properly cleaned up
    const timer = setTimeout(() => {
      // Create new chart
      chartInstance.current = new ChartJS(ctx, {
        type: 'doughnut',
        data: {
          labels: ['Positive', 'Neutral', 'Negative'],
          datasets: [{
            data: [positiveCount, neutralCount, negativeCount],
            backgroundColor: ['#10B981', '#3B82F6', '#EF4444'],
            borderWidth: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              position: 'bottom',
              labels: {
                usePointStyle: true,
                padding: 20
              }
            }
          },
          cutout: '70%'
        }
      });
    }, 0);

    // Cleanup function
    return () => {
      clearTimeout(timer);
      if (chartInstance.current) {
        chartInstance.current.destroy();
        chartInstance.current = null;
      }
    };
  }, [positiveCount, neutralCount, negativeCount]);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
      <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Sentiment Distribution</h3>
      <div className="chart-container">
        <canvas id={chartId} ref={chartRef}></canvas>
      </div>
    </div>
  );
}