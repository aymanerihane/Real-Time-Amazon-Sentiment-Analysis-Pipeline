import { useEffect, useRef } from 'react';
import { Chart as ChartJS } from 'chart.js';

export default function RadarChart() {
  const chartRef = useRef(null);          // For canvas element
  const chartInstance = useRef(null);     // For chart instance

  useEffect(() => {
    // Guard clause if canvas isn't ready
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    
    // Clean up previous chart if exists
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }

    // Create new chart
    chartInstance.current = new ChartJS(ctx, {
      type: 'radar',
      data: {
        labels: ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'Specificity'],
        datasets: [{
          label: 'Model Performance',
          data: [0.92, 0.89, 0.85, 0.87, 0.91],
          backgroundColor: 'rgba(99, 102, 241, 0.2)',
          borderColor: 'rgba(99, 102, 241, 1)',
          borderWidth: 2,
          pointBackgroundColor: 'rgba(99, 102, 241, 1)',
          pointRadius: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          r: {
            angleLines: {
              display: true
            },
            suggestedMin: 0,
            suggestedMax: 1,
            ticks: {
              stepSize: 0.2
            }
          }
        },
        plugins: {
          legend: {
            position: 'top'
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
  }, []);

  return (
    <div className="chart-container">
      <canvas ref={chartRef} />
    </div>
  );
}