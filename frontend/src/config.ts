/**
 * Frontend configuration
 * 
 * Centralized configuration for API endpoints and other settings.
 * In production, you can override these with environment variables.
 */

// API Configuration
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
export const API_ENDPOINTS = {
  upload: `${API_BASE_URL}/upload`,
} as const;

// Power BI Configuration
export const POWER_BI_EMBED_URL = import.meta.env.VITE_POWER_BI_EMBED_URL || '';

// File Upload Configuration
export const UPLOAD_CONFIG = {
  maxFileSize: 200 * 1024 * 1024, // 200MB
  maxFilesPerCategory: 24,
  acceptedTypes: {
    'text/csv': ['.csv'],
    'application/vnd.ms-excel': ['.xls'],
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
  },
} as const;

