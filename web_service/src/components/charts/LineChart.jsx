import { useEffect, useRef } from 'react';
import { Chart as ChartJS, LinearScale, PointElement, LineElement, TimeScale, Tooltip, Legend } from 'chart.js';
import 'chartjs-adapter-date-fns'; // Required for time scale
import { generateTimeSeriesData } from '../../utils/dataUtils';

// Register required components
ChartJS.register(
  LinearScale,
  PointElement,
  LineElement,
  TimeScale,
  Tooltip,
  Legend
);

export default function LineChart() {
  const canvasRef = useRef(null);
  const chartInstance = useRef(null);

  useEffect(() => {
    // Return early if canvas isn't ready
    if (!canvasRef.current) return;

    const ctx = canvasRef.current.getContext('2d');
    if (!ctx) return;

    // Destroy previous chart instance if exists
    const destroyChart = () => {
      if (chartInstance.current) {
        chartInstance.current.destroy();
        chartInstance.current = null;
      }
    };

    destroyChart();

    // Create new chart instance
    try {
      chartInstance.current = new ChartJS(ctx, {
        type: 'line',
        data: {
          datasets: [
            {
              label: 'Positive',
              data: generateTimeSeriesData(30, 5, 15),
              borderColor: '#10B981',
              backgroundColor: 'rgba(16, 185, 129, 0.1)',
              borderWidth: 2,
              tension: 0.3,
              fill: true
            },
            {
              label: 'Neutral',
              data: generateTimeSeriesData(30, 3, 10),
              borderColor: '#3B82F6',
              backgroundColor: 'rgba(59, 130, 246, 0.1)',
              borderWidth: 2,
              tension: 0.3,
              fill: true
            },
            {
              label: 'Negative',
              data: generateTimeSeriesData(30, 1, 8),
              borderColor: '#EF4444',
              backgroundColor: 'rgba(239, 68, 68, 0.1)',
              borderWidth: 2,
              tension: 0.3,
              fill: true
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
                unit: 'day',
                tooltipFormat: 'MMM d'
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
          },
          interaction: {
            mode: 'nearest',
            axis: 'x',
            intersect: false
          }
        }
      });
    } catch (error) {
      console.error('Chart initialization error:', error);
      destroyChart();
    }

    return destroyChart;
  }, []);

  return (
    <div className="chart-container" style={{ position: 'relative', height: '400px', width: '100%' }}>
      <canvas ref={canvasRef} />
    </div>
  );
}