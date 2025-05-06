
export default function DarkModeToggle({ darkMode, toggleDarkMode }) {
    return (
      <button
        id="dark-mode-toggle"
        className="p-2 rounded-full hover:bg-indigo-500"
        onClick={toggleDarkMode}
      >
        <i className={`fas ${darkMode ? 'fa-sun' : 'fa-moon'}`}></i>
      </button>
    );
  }