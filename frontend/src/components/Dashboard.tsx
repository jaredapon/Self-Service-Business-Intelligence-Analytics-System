import React from 'react';
import styles from './Dashboard.module.css';
import { POWER_BI_EMBED_URL } from '../config';

export const Dashboard: React.FC = () => {
  // Power BI embed URL from config
  const powerBiEmbedUrl = POWER_BI_EMBED_URL;
  
  return (
    <div className={styles.dashboard}>
      <div className={styles.dashboardHeader}>
        <h1>Analytics Dashboard</h1>
        <p>Powered by Microsoft Power BI</p>
      </div>
      
      <div className={styles.powerBiContainer}>
        {powerBiEmbedUrl && (
          <iframe
            className={styles.powerBiFrame}
            src={powerBiEmbedUrl}
            frameBorder="0"
            allowFullScreen={true}
            title="Power BI Dashboard"
          />
        )}
        
        {/* Placeholder overlay - shows when Power BI URL is not configured */}
        {!powerBiEmbedUrl && (
          <div className={styles.placeholder}>
          <div className={styles.placeholderContent}>
            <svg 
              className={styles.placeholderIcon}
              xmlns="http://www.w3.org/2000/svg" 
              viewBox="0 0 24 24" 
              fill="none" 
              stroke="currentColor" 
              strokeWidth="2"
            >
              <rect x="3" y="3" width="7" height="7"></rect>
              <rect x="14" y="3" width="7" height="7"></rect>
              <rect x="14" y="14" width="7" height="7"></rect>
              <rect x="3" y="14" width="7" height="7"></rect>
            </svg>
            <h2>Power BI Dashboard Placeholder</h2>
            <p>Configure your Power BI embed URL to display the dashboard here.</p>
            <div className={styles.instructions}>
              <h3>How to configure your Power BI embed URL:</h3>
              <ol>
                <li>Open your report in Power BI Service</li>
                <li>Click <strong>File → Embed report → Publish to web</strong></li>
                <li>Copy the iframe src URL</li>
                <li>Add it to your <code>.env</code> file: <code>VITE_POWER_BI_EMBED_URL=your_url_here</code></li>
                <li>Restart the development server</li>
              </ol>
            </div>
          </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;