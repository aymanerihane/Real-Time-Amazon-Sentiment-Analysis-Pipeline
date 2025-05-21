import { useState, useEffect } from 'react';
import DarkModeToggle from './DarkModeToggle'; // Assuming you have a DarkModeToggle component

export default function Header({ darkMode, toggleDarkMode, activeTab, setActiveTab }) {
  const [connectionStatus, setConnectionStatus] = useState('Connected');

  // Simulate connection status changes
  useEffect(() => {
    const statuses = ['Connected', 'Connecting...', 'Disconnected'];
    let index = 0;
    
    const ws = new WebSocket('ws://localhost:8006/ws/reviews'); // Adjust URL as needed
    
    ws.onopen = () => setConnectionStatus('Connected');
    ws.onclose = () => setConnectionStatus('Disconnected');
    ws.onerror = () => setConnectionStatus('Disconnected');
    
    // Ping server every 5 seconds to check connection
    const interval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
      setConnectionStatus('Connected');
      } else {
      setConnectionStatus('Disconnected');
      }
    }, 5000);

    // Clean up WebSocket connection
    return () => {
      ws.close();
      clearInterval(interval);
    };

    // return () => clearInterval(interval);
  }, []);

  // Determine connection status color
  const getStatusColor = () => {
    switch(connectionStatus) {
      case 'Connected': return 'bg-green-500';
      case 'Connecting...': return 'bg-yellow-500';
      case 'Disconnected': return 'bg-red-500';
      default: return 'bg-gray-500';
    }
  };

  return (
    <header className="bg-indigo-600 text-white shadow-lg">
      <div className="container mx-auto px-4 py-6">
        <div className="flex justify-between items-center">
          <div className="flex items-center space-x-4">
            <i className="fas fa-chart-line text-2xl"></i>
            <h1 className="text-2xl font-bold">Sentiment Analysis Dashboard</h1>
          </div>
          
          <div className="flex items-center space-x-4">
            {/* Dark/Light Mode Toggle */}
            {/* <DarkModeToggle darkMode={darkMode} toggleDarkMode={toggleDarkMode} /> */}
            {/* Connection Status Indicator */}
            <div className="flex items-center">
              <span className="relative flex h-3 w-3 mr-2">
                <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${getStatusColor()} opacity-75`}></span>
                <span className={`relative inline-flex rounded-full h-3 w-3 ${getStatusColor()}`}></span>
              </span>
              <span id="connection-status" className="text-sm font-medium">
                {connectionStatus}
              </span>
            </div>
            
            
          </div>
        </div>
        
        {/* Tab Navigation */}
        <div className="mt-4 border-b border-white/20">
          <nav className="-mb-px flex space-x-8">
            <button 
              onClick={() => setActiveTab('realtime')}
              className={`whitespace-nowrap py-2 px-4 border-b-2 font-medium text-sm ${
                activeTab === 'realtime' 
                  ? 'border-white text-white' 
                  : 'border-transparent text-white/70 hover:text-white/90'
              }`}
            >
              Real-time Feed
            </button>
            <button 
              onClick={() => setActiveTab('dashboard')}
              className={`whitespace-nowrap py-2 px-4 border-b-2 font-medium text-sm ${
                activeTab === 'dashboard' 
                  ? 'border-white text-white' 
                  : 'border-transparent text-white/70 hover:text-white/90'
              }`}
            >
              Analytics Dashboard
            </button>
            <button 
              onClick={() => setActiveTab('reviews')}
              className={`whitespace-nowrap py-2 px-4 border-b-2 font-medium text-sm ${
                activeTab === 'reviews' 
                  ? 'border-white text-white' 
                  : 'border-transparent text-white/70 hover:text-white/90'
              }`}
            >
              Offline Reviews
            </button>
          </nav>
        </div>
      </div>
    </header>
  );
}