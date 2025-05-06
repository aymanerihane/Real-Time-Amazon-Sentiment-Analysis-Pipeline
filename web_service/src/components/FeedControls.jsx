export default function FeedControls({ isPaused, setIsPaused, asinFilter, setAsinFilter }) {
    return (
      <div className="flex items-center space-x-4">
        <div className="relative">
          <input
            type="text"
            id="asin-filter"
            placeholder="Filter by ASIN..."
            className="pl-10 pr-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
            value={asinFilter}
            onChange={(e) => setAsinFilter(e.target.value)}
          />
          <i className="fas fa-search absolute left-3 top-3 text-gray-400"></i>
        </div>
        <button
          id="pause-feed"
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg transition ${
            isPaused
              ? 'bg-indigo-100 hover:bg-indigo-200 text-indigo-700 dark:bg-indigo-900 dark:hover:bg-indigo-800 dark:text-indigo-200'
              : 'bg-gray-100 hover:bg-gray-200 text-gray-700 dark:bg-gray-700 dark:hover:bg-gray-600 dark:text-gray-200'
          }`}
          onClick={() => setIsPaused(!isPaused)}
        >
          <i className={`fas ${isPaused ? 'fa-play' : 'fa-pause'}`}></i>
          <span>{isPaused ? 'Resume' : 'Pause'}</span>
        </button>
      </div>
    );
  }