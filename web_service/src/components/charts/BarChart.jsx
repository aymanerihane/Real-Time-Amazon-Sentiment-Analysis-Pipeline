import { useEffect, useRef } from 'react';
import { Chart as ChartJS } from 'chart.js';
import { sampleASINs } from '../../utils/dataUtils';

export default function BarChart() {
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
      type: 'bar',
      data: {
        labels: sampleASINs,
        datasets: [
          {
            label: 'Positive',
            data: sampleASINs.map(() => Math.floor(Math.random() * 50) + 20),
            backgroundColor: '#10B981',
            borderRadius: 4
          },
          {
            label: 'Neutral',
            data: sampleASINs.map(() => Math.floor(Math.random() * 30) + 10),
            backgroundColor: '#3B82F6',
            borderRadius: 4
          },
          {
            label: 'Negative',
            data: sampleASINs.map(() => Math.floor(Math.random() * 20) + 5),
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
            intersect: false
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