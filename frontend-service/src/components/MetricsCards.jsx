export default function MetricsCards({ positiveCount, neutralCount, negativeCount }) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <MetricCard
          title="Total Reviews"
          value={positiveCount + neutralCount + negativeCount}
          icon="fa-comments"
          color="indigo"
        />
        <MetricCard
          title="Positive"
          value={positiveCount}
          icon="fa-smile"
          color="green"
        />
        <MetricCard
          title="Neutral"
          value={neutralCount}
          icon="fa-meh"
          color="blue"
        />
        <MetricCard
          title="Negative"
          value={negativeCount}
          icon="fa-frown"
          color="red"
        />
      </div>
    );
  }
  
  function MetricCard({ title, value, icon, color }) {
    const colorClasses = {
      indigo: {
        bg: 'bg-indigo-50 dark:bg-indigo-900/30',
        text: 'text-indigo-600 dark:text-indigo-400',
        iconBg: 'bg-indigo-100 dark:bg-indigo-800'
      },
      green: {
        bg: 'bg-green-50 dark:bg-green-900/30',
        text: 'text-green-600 dark:text-green-400',
        iconBg: 'bg-green-100 dark:bg-green-800'
      },
      blue: {
        bg: 'bg-blue-50 dark:bg-blue-900/30',
        text: 'text-blue-600 dark:text-blue-400',
        iconBg: 'bg-blue-100 dark:bg-blue-800'
      },
      red: {
        bg: 'bg-red-50 dark:bg-red-900/30',
        text: 'text-red-600 dark:text-red-400',
        iconBg: 'bg-red-100 dark:bg-red-800'
      }
    };
  
    return (
      <div className={`${colorClasses[color].bg} rounded-lg p-4 shadow-sm`}>
        <div className="flex justify-between items-start">
          <div>
            <p className={`text-sm ${colorClasses[color].text} font-medium`}>{title}</p>
            <h3 className="text-2xl font-bold text-gray-800 dark:text-gray-200 mt-1">
              {value}
            </h3>
          </div>
          <div className={`${colorClasses[color].iconBg} p-2 rounded-lg`}>
            <i className={`fas ${icon} ${colorClasses[color].text}`}></i>
          </div>
        </div>
      </div>
    );
  }