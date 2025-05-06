import { sampleASINs } from '../utils/dataUtils';

export default function DashboardFilters() {
  const applyFilters = () => {
    alert('Filters applied! In a real application, this would update the dashboard data.');
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
      <div>
        <label htmlFor="time-range" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Time Range</label>
        <select
          id="time-range"
          className="w-full border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
        >
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
          <option value="90d">Last 90 days</option>
          <option value="all">All time</option>
        </select>
      </div>
      <div>
        <label htmlFor="asin-select" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">ASIN Filter</label>
        <select
          id="asin-select"
          className="w-full border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
        >
          <option value="all">All Products</option>
          {sampleASINs.map(asin => (
            <option key={asin} value={asin}>{asin}</option>
          ))}
        </select>
      </div>
      <div>
        <label htmlFor="sentiment-filter" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Sentiment</label>
        <select
          id="sentiment-filter"
          className="w-full border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
        >
          <option value="all">All Sentiments</option>
          <option value="positive">Positive</option>
          <option value="neutral">Neutral</option>
          <option value="negative">Negative</option>
        </select>
      </div>
      <div className="flex items-end">
        <button
          id="apply-filters"
          className="w-full bg-indigo-600 text-white px-4 py-2 rounded-lg hover:bg-indigo-700 transition dark:bg-indigo-700 dark:hover:bg-indigo-600"
          onClick={applyFilters}
        >
          Apply Filters
        </button>
      </div>
    </div>
  );
}