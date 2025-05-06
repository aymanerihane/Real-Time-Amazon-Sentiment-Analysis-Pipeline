export default function SentimentStats({ positiveCount, neutralCount, negativeCount }) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 shadow-sm">
        <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-3">Recent Activity</h3>
        <div className="space-y-3">
          <StatItem label="Positive" count={positiveCount} color="green" />
          <StatItem label="Neutral" count={neutralCount} color="blue" />
          <StatItem label="Negative" count={negativeCount} color="red" />
        </div>
      </div>
    );
  }
  
  function StatItem({ label, count, color }) {
    const colorClasses = {
      green: 'bg-green-500',
      blue: 'bg-blue-500',
      red: 'bg-red-500'
    };
  
    return (
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <span className={`h-2 w-2 rounded-full ${colorClasses[color]}`}></span>
          <span className="text-sm dark:text-gray-300">{label}</span>
        </div>
        <span className="text-sm font-medium dark:text-gray-200">{count}</span>
      </div>
    );
  }