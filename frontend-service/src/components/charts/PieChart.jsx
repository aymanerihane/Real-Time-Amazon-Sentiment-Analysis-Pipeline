import { useEffect, useRef } from 'react';
import { Chart as ChartJS } from 'chart.js';

export default function PieChart({ positiveCount, neutralCount, negativeCount }) {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);

  useEffect(() => {
    // Ensure the canvas element exists
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    
    // Destroy previous chart if it exists
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }

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

    // Cleanup function
    return () => {
      if (chartInstance.current) {
        chartInstance.current.destroy();
      }
    };
  }, [positiveCount, neutralCount, negativeCount]);

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
      <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Sentiment Distribution</h3>
      <div className="chart-container">
        <canvas ref={chartRef}></canvas>
      </div>
    </div>
  );
}