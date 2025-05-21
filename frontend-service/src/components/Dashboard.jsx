import { useState } from 'react';
import LineChart from './charts/LineChart';
import RadarChart from './charts/RadarChart';
import BarChart from './charts/BarChart';
import DashboardFilters from './DashboardFilters';
import MetricsCards from './MetricsCards';

export default function Dashboard({ positiveCount, neutralCount, negativeCount }) {
  const [filters, setFilters] = useState({
    timeRange: '7d',
    asin: 'all',
    sentiment: 'all'
  });

  const handleFilterChange = (newFilters) => {
    console.log("Filters changed:", newFilters);
    setFilters(newFilters);
  };

  return (
    <div className="space-y-6">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        <h2 className="text-xl font-semibold text-gray-800 dark:text-gray-200 mb-6">
          Sentiment Analysis Analytics
        </h2>
        
        {/* <DashboardFilters onFilterChange={handleFilterChange} /> */}
        <MetricsCards 
          positiveCount={positiveCount}
          neutralCount={neutralCount}
          negativeCount={negativeCount}
          filters={filters}
        />
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
            <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Sentiment Over Time</h3>
            <div className="chart-container">
              <LineChart filters={filters} />
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
            <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Model Performance Metrics</h3>
            <div className="chart-container">
              <RadarChart filters={filters} />
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
            <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">ASIN Comparison</h3>
            <div className="chart-container">
              <BarChart filters={filters} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}