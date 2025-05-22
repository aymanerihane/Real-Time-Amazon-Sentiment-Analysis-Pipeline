import { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS, registerables } from 'chart.js';

// Register the chart components
ChartJS.register(...registerables);

export default function RadarChart() {
  const chartRef = useRef(null);          // For canvas element
  const chartInstance = useRef(null);     // For chart instance
  // Generate a truly unique ID for this chart instance
  const [chartId] = useState(`radar-chart-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`);

  // Function to safely destroy chart
  const destroyChart = () => {
    if (chartInstance.current) {
      chartInstance.current.destroy();
      chartInstance.current = null;
    }
  };

  useEffect(() => {
    // First destroy any existing chart
    destroyChart();

    // Guard clause if canvas isn't ready
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
          type: 'radar',
          data: {
            labels: ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'Specificity'],
            datasets: [{
              label: 'Model Performance',
              data: [0.99, 0.99, 0.99, 0.99, 0.99],
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
      } catch (error) {
        console.error("Chart creation error:", error);
      }
    }, 100); // Longer delay to ensure cleanup

    // Cleanup function
    return () => {
      clearTimeout(timer);
      destroyChart();
    };
  }, [chartId]); // Re-render when chartId changes

  return (
    <div className="chart-container" style={{ height: "300px" }}>
      <canvas id={chartId} ref={chartRef} />
    </div>
  );
}