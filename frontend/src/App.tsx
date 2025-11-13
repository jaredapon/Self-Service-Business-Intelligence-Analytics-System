import { useState } from 'react';
import Navigation from './components/Navigation';
import Dashboard from './components/Dashboard';
import DataUpload from './components/DataUpload';
import './App.css';

function App() {
  const [currentPage, setCurrentPage] = useState('dashboard');

  const renderCurrentPage = () => {
    switch (currentPage) {
      case 'dashboard':
        return <Dashboard />;
      case 'upload':
        return <DataUpload />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="App">
      <Navigation
        currentPage={currentPage}
        onPageChange={setCurrentPage}
        userName="BookLatte"
      />
      
      <main className="main-content">
        {renderCurrentPage()}
      </main>
    </div>
  );
}

export default App;
