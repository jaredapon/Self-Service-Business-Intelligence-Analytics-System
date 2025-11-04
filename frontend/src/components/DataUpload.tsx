import React, { useState, useCallback, useRef } from 'react';
import { 
  MdUploadFile, 
  MdDelete,
  MdDescription,
  MdTableChart,
  MdWarning
} from 'react-icons/md';
import ConfirmationModal from './ConfirmationModal';
import styles from './DataUpload.module.css';

interface FileWithPreview extends File {
  id: string;
  preview?: string;
}

type FileCategory = 'raw_sales' | 'sales_by_product';

const ACCEPTED_FILE_TYPES = {
  'text/csv': ['.csv'],
  'application/vnd.ms-excel': ['.xls'],
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
};

const MAX_FILE_SIZE = 200 * 1024 * 1024; // 200MB
const MAX_FILES_PER_CATEGORY = 12;

export const DataUpload: React.FC = () => {
  const [rawSalesFiles, setRawSalesFiles] = useState<FileWithPreview[]>([]);
  const [salesByProductFiles, setSalesByProductFiles] = useState<FileWithPreview[]>([]);
  const [isDragOverRawSales, setIsDragOverRawSales] = useState(false);
  const [isDragOverSalesByProduct, setIsDragOverSalesByProduct] = useState(false);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState<Record<string, number>>({});
  const [errors, setErrors] = useState<string[]>([]);
  const rawSalesInputRef = useRef<HTMLInputElement>(null);
  const salesByProductInputRef = useRef<HTMLInputElement>(null);
  const [selectedRawSalesIds, setSelectedRawSalesIds] = useState<Set<string>>(new Set());
  const [selectedSalesByProductIds, setSelectedSalesByProductIds] = useState<Set<string>>(new Set());
  const [showRemoveSelectedConfirm, setShowRemoveSelectedConfirm] = useState(false);
  const [showClearAllConfirm, setShowClearAllConfirm] = useState(false);
  const [filePendingRemoval, setFilePendingRemoval] = useState<{ file: FileWithPreview; category: FileCategory } | null>(null);
  const [categoryToClear, setCategoryToClear] = useState<FileCategory | 'all' | null>(null);

  const cleanFileName = useCallback((name: string) => {
    const dot = name.lastIndexOf('.');
    const base = dot > 0 ? name.slice(0, dot) : name;
    return base.replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim();
  }, []);

  const getExtension = (name: string) => name.split('.').pop()?.toLowerCase() || 'unknown';

  const getDisplayName = useCallback((name: string) => {
    const base = cleanFileName(name);
    const ext = getExtension(name);
    return ext === 'unknown' ? base : `${base}.${ext}`;
  }, [cleanFileName]);

  const formatBytes = useCallback((bytes: number) => {
    if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KB`;
    return `${bytes} B`;
  }, []);

  const generateFileId = () => `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  const validateFile = (file: File): string | null => {
    if (file.size > MAX_FILE_SIZE) {
      return `File "${file.name}" is too large. Maximum size is ${MAX_FILE_SIZE / 1024 / 1024}MB.`;
    }

    const acceptedTypes = Object.keys(ACCEPTED_FILE_TYPES);
    const acceptedExtensions = Object.values(ACCEPTED_FILE_TYPES).flat();
    
    const fileExtension = '.' + file.name.split('.').pop()?.toLowerCase();
    const isValidType = acceptedTypes.includes(file.type) || acceptedExtensions.includes(fileExtension);
    
    if (!isValidType) {
      return `File "${file.name}" has an unsupported format. Accepted formats: CSV, Excel (.xls, .xlsx)`;
    }

    return null;
  };

  const processFiles = useCallback((fileList: FileList, category: FileCategory) => {
    const newErrors: string[] = [];
    const validFiles: FileWithPreview[] = [];

    const currentFiles = category === 'raw_sales' ? rawSalesFiles : salesByProductFiles;

    if (currentFiles.length + fileList.length > MAX_FILES_PER_CATEGORY) {
      newErrors.push(`Cannot upload more than ${MAX_FILES_PER_CATEGORY} files per category.`);
      setErrors(newErrors);
      return;
    }

    Array.from(fileList).forEach((file) => {
      const error = validateFile(file);
      if (error) {
        newErrors.push(error);
      } else {
        const fileWithId = Object.assign(file, { id: generateFileId() }) as FileWithPreview;
        validFiles.push(fileWithId);
      }
    });

    if (newErrors.length > 0) {
      setErrors(newErrors);
    } else {
      setErrors([]);
    }

    if (category === 'raw_sales') {
      setRawSalesFiles(prev => [...prev, ...validFiles]);
    } else {
      setSalesByProductFiles(prev => [...prev, ...validFiles]);
    }
  }, [rawSalesFiles, salesByProductFiles]);

  const handleDragEnter = useCallback((e: React.DragEvent, category: FileCategory) => {
    e.preventDefault();
    e.stopPropagation();
    if (category === 'raw_sales') {
      setIsDragOverRawSales(true);
    } else {
      setIsDragOverSalesByProduct(true);
    }
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent, category: FileCategory) => {
    e.preventDefault();
    e.stopPropagation();
    if (category === 'raw_sales') {
      setIsDragOverRawSales(false);
    } else {
      setIsDragOverSalesByProduct(false);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent, category: FileCategory) => {
    e.preventDefault();
    e.stopPropagation();
    if (category === 'raw_sales') {
      setIsDragOverRawSales(false);
    } else {
      setIsDragOverSalesByProduct(false);
    }

    const droppedFiles = e.dataTransfer.files;
    if (droppedFiles.length > 0) {
      processFiles(droppedFiles, category);
    }
  }, [processFiles]);

  const handleFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>, category: FileCategory) => {
    const selectedFiles = e.target.files;
    if (selectedFiles && selectedFiles.length > 0) {
      processFiles(selectedFiles, category);
    }
  }, [processFiles]);

  // Internal single remove logic and also update selected set
  const removeFile = useCallback((fileId: string, category: FileCategory) => {
    if (category === 'raw_sales') {
      setRawSalesFiles(prev => prev.filter(file => file.id !== fileId));
      setSelectedRawSalesIds(prev => {
        const next = new Set(prev);
        next.delete(fileId);
        return next;
      });
    } else {
      setSalesByProductFiles(prev => prev.filter(file => file.id !== fileId));
      setSelectedSalesByProductIds(prev => {
        const next = new Set(prev);
        next.delete(fileId);
        return next;
      });
    }
    setErrors(prev => prev.filter(error => !error.includes(fileId)));
  }, []);

  // Clear all files
  const clearAllFiles = useCallback((category?: FileCategory) => {
    if (!category || category === 'raw_sales') {
      setRawSalesFiles([]);
      setSelectedRawSalesIds(new Set());
      if (rawSalesInputRef.current) {
        rawSalesInputRef.current.value = '';
      }
    }
    if (!category || category === 'sales_by_product') {
      setSalesByProductFiles([]);
      setSelectedSalesByProductIds(new Set());
      if (salesByProductInputRef.current) {
        salesByProductInputRef.current.value = '';
      }
    }
    setErrors([]);
  }, []);

  // Selection helpers for raw sales
  const isRawSalesSelected = useCallback((id: string) => selectedRawSalesIds.has(id), [selectedRawSalesIds]);
  const toggleRawSalesSelect = useCallback((id: string) => {
    setSelectedRawSalesIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);
  const selectAllRawSales = useCallback(() => {
    setSelectedRawSalesIds(new Set(rawSalesFiles.map(f => f.id)));
  }, [rawSalesFiles]);
  const clearRawSalesSelection = useCallback(() => setSelectedRawSalesIds(new Set()), []);
  const allRawSalesSelected = rawSalesFiles.length > 0 && selectedRawSalesIds.size === rawSalesFiles.length;

  // Selection helpers for sales by product
  const isSalesByProductSelected = useCallback((id: string) => selectedSalesByProductIds.has(id), [selectedSalesByProductIds]);
  const toggleSalesByProductSelect = useCallback((id: string) => {
    setSelectedSalesByProductIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);
  const selectAllSalesByProduct = useCallback(() => {
    setSelectedSalesByProductIds(new Set(salesByProductFiles.map(f => f.id)));
  }, [salesByProductFiles]);
  const clearSalesByProductSelection = useCallback(() => setSelectedSalesByProductIds(new Set()), []);
  const allSalesByProductSelected = salesByProductFiles.length > 0 && selectedSalesByProductIds.size === salesByProductFiles.length;

  const handleUpload = useCallback(async () => {
    const allFiles = [...rawSalesFiles, ...salesByProductFiles];
    if (allFiles.length === 0) return;
    
    setIsUploading(true);
    
    try {
      // Simulate upload progress for each file
      for (const file of allFiles) {
        setUploadProgress(prev => ({ ...prev, [file.id]: 0 }));
        
        // Simulate upload progress
        for (let progress = 0; progress <= 100; progress += 10) {
          await new Promise(resolve => setTimeout(resolve, 100));
          setUploadProgress(prev => ({ ...prev, [file.id]: progress }));
        }
      }
      
      // Here you would make actual API calls to upload files
      console.log('Uploading raw sales files:', rawSalesFiles);
      console.log('Uploading sales by product files:', salesByProductFiles);
      
      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Reset state after successful upload
      setRawSalesFiles([]);
      setSalesByProductFiles([]);
      setUploadProgress({});
      setShowConfirmation(false);
      
      // Show success message (you can implement a toast notification here)
      alert('Files uploaded successfully!');
      
    } catch (error) {
      console.error('Upload failed:', error);
      setErrors(['Upload failed. Please try again.']);
    } finally {
      setIsUploading(false);
    }
  }, [rawSalesFiles, salesByProductFiles]);

  const getFileIcon = (fileName: string) => {
    const extension = fileName.split('.').pop()?.toLowerCase();
    
    switch (extension) {
      case 'csv':
      case 'xlsx':
      case 'xls':
        return <MdTableChart size={24} />;
      default:
        return <MdDescription size={24} />;
    }
  };

  const openFileDialog = (category: FileCategory) => {
    if (category === 'raw_sales') {
      rawSalesInputRef.current?.click();
    } else {
      salesByProductInputRef.current?.click();
    }
  };

  // Bulk remove selected confirm action
  const confirmRemoveSelected = () => {
    setRawSalesFiles(prev => prev.filter(f => !selectedRawSalesIds.has(f.id)));
    setSalesByProductFiles(prev => prev.filter(f => !selectedSalesByProductIds.has(f.id)));
    setSelectedRawSalesIds(new Set());
    setSelectedSalesByProductIds(new Set());
    setShowRemoveSelectedConfirm(false);
  };

  // Single file remove confirm action
  const confirmRemoveSingle = () => {
    if (!filePendingRemoval) return;
    removeFile(filePendingRemoval.file.id, filePendingRemoval.category);
    setFilePendingRemoval(null);
  };

  // Clear all confirm action
  const confirmClearAll = () => {
    if (categoryToClear === 'raw_sales') {
      if (selectedRawSalesIds.size > 0) {
        // Remove only selected files
        setRawSalesFiles(prev => prev.filter(f => !selectedRawSalesIds.has(f.id)));
        setSelectedRawSalesIds(new Set());
      } else {
        // Clear all files
        clearAllFiles('raw_sales');
      }
    } else if (categoryToClear === 'sales_by_product') {
      if (selectedSalesByProductIds.size > 0) {
        // Remove only selected files
        setSalesByProductFiles(prev => prev.filter(f => !selectedSalesByProductIds.has(f.id)));
        setSelectedSalesByProductIds(new Set());
      } else {
        // Clear all files
        clearAllFiles('sales_by_product');
      }
    } else {
      // Clear all files from all categories
      clearAllFiles(categoryToClear === 'all' ? undefined : categoryToClear || undefined);
    }
    setShowClearAllConfirm(false);
    setCategoryToClear(null);
  };

  // Helpers
  const allFiles = [...rawSalesFiles, ...salesByProductFiles];
  const totalSizeFormatted = formatBytes(allFiles.reduce((sum, f) => sum + f.size, 0));
  
  // Validation: Both categories must have at least one file
  const hasRawSalesFiles = rawSalesFiles.length > 0;
  const hasSalesByProductFiles = salesByProductFiles.length > 0;
  const canUpload = hasRawSalesFiles && hasSalesByProductFiles;

  return (
    <div className={styles.dataUploadContainer}>
      <div className={styles.dataUploadHeader}>
        <h1>Data Upload</h1>
        <p>Upload your data files for analysis. Supported formats: CSV, Excel (.xls, .xlsx)</p>
      </div>

      {/* Error Messages */}
      {errors.length > 0 && (
        <div className={styles.dataUploadErrorContainer}>
          {errors.map((error, index) => (
            <div key={index} className={styles.dataUploadErrorMessage}>
              {error}
            </div>
          ))}
        </div>
      )}

      {/* Two Upload Sections */}
      <div className={styles.dataUploadSections}>
        {/* Raw Sales Files Section */}
        <div className={styles.dataUploadSection}>
          <h2 className={styles.dataUploadSectionTitle}>Raw Sales Files</h2>
          <p className={styles.dataUploadSectionDescription}>Upload raw sales transaction files</p>
          
          <div
            className={`${styles.dataUploadDropZone} ${isDragOverRawSales ? styles.dragOver : ''} ${rawSalesFiles.length > 0 ? styles.hasFiles : ''}`}
            onDragEnter={(e) => handleDragEnter(e, 'raw_sales')}
            onDragLeave={(e) => handleDragLeave(e, 'raw_sales')}
            onDragOver={handleDragOver}
            onDrop={(e) => handleDrop(e, 'raw_sales')}
            onClick={() => openFileDialog('raw_sales')}
          >
            <input
              ref={rawSalesInputRef}
              type="file"
              multiple
              accept={Object.values(ACCEPTED_FILE_TYPES).flat().join(',')}
              onChange={(e) => handleFileInput(e, 'raw_sales')}
              className={styles.dataUploadHiddenInput}
            />
            
            <div className={styles.dataUploadDropZoneContent}>
              <div className={styles.dataUploadIcon}>
                <MdUploadFile size={48} color="black"/>
              </div>
              <h3>Drop raw sales files here or click to browse</h3>
              <p>Support for CSV and Excel files</p>
              <p className={styles.dataUploadLimits}>
                Maximum {MAX_FILES_PER_CATEGORY} files • {MAX_FILE_SIZE / 1024 / 1024}MB per file
              </p>
            </div>
          </div>

          {/* Raw Sales File List */}
          {(rawSalesFiles.length > 0 || salesByProductFiles.length > 0) && (
            <div className={styles.dataUploadFileList}>
              <div className={styles.dataUploadFileListHeader}>
                <div className={styles.dataUploadFileListHeaderLeft}>
                  <h3>Selected Files ({rawSalesFiles.length})</h3>
                  {rawSalesFiles.length > 0 && (
                    <label className={styles.dataUploadSelectAll}>
                      <input
                        type="checkbox"
                        className={styles.dataUploadFileCheckbox}
                        checked={allRawSalesSelected}
                        onChange={() => (allRawSalesSelected ? clearRawSalesSelection() : selectAllRawSales())}
                      />
                      <span style={{ marginRight: '0.5rem' }}>
                        {allRawSalesSelected ? 'Unselect all' : 'Select all'}
                      </span>
                    </label>
                  )}
                </div>
                <div className={styles.dataUploadHeaderActions}>
                  <button
                    className={styles.dataUploadSecondaryButton}
                    onClick={() => openFileDialog('raw_sales')}
                    disabled={isUploading}
                  >
                    Add Files
                  </button>
                  {rawSalesFiles.length > 0 && (
                    <button
                      className={styles.dataUploadClearAllButton}
                      onClick={() => {
                        setCategoryToClear('raw_sales');
                        setShowClearAllConfirm(true);
                      }}
                      disabled={isUploading}
                    >
                      <MdDelete style={{ marginRight: '4px' }} />
                      {selectedRawSalesIds.size > 0 ? `Remove Selected (${selectedRawSalesIds.size})` : 'Clear All'}
                    </button>
                  )}
                </div>
              </div>

              {rawSalesFiles.length > 0 && (
                <div className={styles.dataUploadFileItems}>
                {rawSalesFiles.map((file) => (
                  <div
                    key={file.id}
                    className={`${styles.dataUploadFileItem} ${isRawSalesSelected(file.id) ? styles.selected : ''}`}
                  >
                    <input
                      type="checkbox"
                      className={styles.dataUploadFileCheckbox}
                      checked={isRawSalesSelected(file.id)}
                      onChange={() => toggleRawSalesSelect(file.id)}
                      aria-label={`Select ${file.name}`}
                    />
                    <div className={styles.dataUploadFileIcon}>
                      {getFileIcon(file.name)}
                    </div>

                    <div className={styles.dataUploadFileDetails}>
                      <div
                        className={styles.dataUploadFileName}
                        title={file.name}
                      >
                        {getDisplayName(file.name)}
                      </div>
                      <div className={styles.dataUploadFileMetadata}>
                        <span>{formatBytes(file.size)}</span>
                      </div>

                      {uploadProgress[file.id] !== undefined && (
                        <div className={styles.dataUploadProgressBar}>
                          <div
                            className={styles.dataUploadProgressFill}
                            style={{ width: `${uploadProgress[file.id]}%` }}
                          />
                          <span className={styles.dataUploadProgressText}>
                            {uploadProgress[file.id]}%
                          </span>
                        </div>
                      )}
                    </div>

                    <button
                      className={styles.dataUploadRemoveButton}
                      onClick={() => setFilePendingRemoval({ file, category: 'raw_sales' })}
                      disabled={isUploading}
                      aria-label={`Remove ${file.name}`}
                      title="Remove file"
                    >
                      x
                    </button>
                  </div>
                ))}
              </div>
              )}
            </div>
          )}
        </div>

        {/* Sales by Product Files Section */}
        <div className={styles.dataUploadSection}>
          <h2 className={styles.dataUploadSectionTitle}>Sales by Product Files</h2>
          <p className={styles.dataUploadSectionDescription}>Upload sales by product report files</p>
          
          <div
            className={`${styles.dataUploadDropZone} ${isDragOverSalesByProduct ? styles.dragOver : ''} ${salesByProductFiles.length > 0 ? styles.hasFiles : ''}`}
            onDragEnter={(e) => handleDragEnter(e, 'sales_by_product')}
            onDragLeave={(e) => handleDragLeave(e, 'sales_by_product')}
            onDragOver={handleDragOver}
            onDrop={(e) => handleDrop(e, 'sales_by_product')}
            onClick={() => openFileDialog('sales_by_product')}
          >
            <input
              ref={salesByProductInputRef}
              type="file"
              multiple
              accept={Object.values(ACCEPTED_FILE_TYPES).flat().join(',')}
              onChange={(e) => handleFileInput(e, 'sales_by_product')}
              className={styles.dataUploadHiddenInput}
            />
            
            <div className={styles.dataUploadDropZoneContent}>
              <div className={styles.dataUploadIcon}>
                <MdUploadFile size={48} color="black"/>
              </div>
              <h3>Drop sales by product files here or click to browse</h3>
              <p>Support for CSV and Excel files</p>
              <p className={styles.dataUploadLimits}>
                Maximum {MAX_FILES_PER_CATEGORY} files • {MAX_FILE_SIZE / 1024 / 1024}MB per file
              </p>
            </div>
          </div>

          {/* Sales by Product File List */}
          {(salesByProductFiles.length > 0 || rawSalesFiles.length > 0) && (
            <div className={styles.dataUploadFileList}>
              <div className={styles.dataUploadFileListHeader}>
                <div className={styles.dataUploadFileListHeaderLeft}>
                  <h3>Selected Files ({salesByProductFiles.length})</h3>
                  {salesByProductFiles.length > 0 && (
                    <label className={styles.dataUploadSelectAll}>
                      <input
                        type="checkbox"
                        className={styles.dataUploadFileCheckbox}
                        checked={allSalesByProductSelected}
                        onChange={() => (allSalesByProductSelected ? clearSalesByProductSelection() : selectAllSalesByProduct())}
                      />
                      <span style={{ marginRight: '0.5rem' }}>
                        {allSalesByProductSelected ? 'Unselect all' : 'Select all'}
                      </span>
                    </label>
                  )}
                </div>
                <div className={styles.dataUploadHeaderActions}>
                  <button
                    className={styles.dataUploadSecondaryButton}
                    onClick={() => openFileDialog('sales_by_product')}
                    disabled={isUploading}
                  >
                    Add Files
                  </button>
                  {salesByProductFiles.length > 0 && (
                    <button
                      className={styles.dataUploadClearAllButton}
                      onClick={() => {
                        setCategoryToClear('sales_by_product');
                        setShowClearAllConfirm(true);
                      }}
                      disabled={isUploading}
                    >
                      <MdDelete style={{ marginRight: '4px' }} />
                      {selectedSalesByProductIds.size > 0 ? `Remove Selected (${selectedSalesByProductIds.size})` : 'Clear All'}
                    </button>
                  )}
                </div>
              </div>

              {salesByProductFiles.length > 0 && (
                <div className={styles.dataUploadFileItems}>
                {salesByProductFiles.map((file) => (
                  <div
                    key={file.id}
                    className={`${styles.dataUploadFileItem} ${isSalesByProductSelected(file.id) ? styles.selected : ''}`}
                  >
                    <input
                      type="checkbox"
                      className={styles.dataUploadFileCheckbox}
                      checked={isSalesByProductSelected(file.id)}
                      onChange={() => toggleSalesByProductSelect(file.id)}
                      aria-label={`Select ${file.name}`}
                    />
                    <div className={styles.dataUploadFileIcon}>
                      {getFileIcon(file.name)}
                    </div>

                    <div className={styles.dataUploadFileDetails}>
                      <div
                        className={styles.dataUploadFileName}
                        title={file.name}
                      >
                        {getDisplayName(file.name)}
                      </div>
                      <div className={styles.dataUploadFileMetadata}>
                        <span>{formatBytes(file.size)}</span>
                      </div>

                      {uploadProgress[file.id] !== undefined && (
                        <div className={styles.dataUploadProgressBar}>
                          <div
                            className={styles.dataUploadProgressFill}
                            style={{ width: `${uploadProgress[file.id]}%` }}
                          />
                          <span className={styles.dataUploadProgressText}>
                            {uploadProgress[file.id]}%
                          </span>
                        </div>
                      )}
                    </div>

                    <button
                      className={styles.dataUploadRemoveButton}
                      onClick={() => setFilePendingRemoval({ file, category: 'sales_by_product' })}
                      disabled={isUploading}
                      aria-label={`Remove ${file.name}`}
                      title="Remove file"
                    >
                      x
                    </button>
                  </div>
                ))}
              </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Upload Actions */}
      {allFiles.length > 0 && (
        <div className={styles.dataUploadActions}>
          <button
            className={styles.dataUploadButton}
            onClick={() => setShowConfirmation(true)}
            disabled={isUploading || !canUpload}
          >
            Upload {allFiles.length} File{allFiles.length > 1 ? 's' : ''}
          </button>
          {!canUpload && (
            <p className={styles.dataUploadValidationMessage}>
              Both Raw Sales and Sales by Product files are required to proceed with upload.
              {!hasRawSalesFiles && ' Missing: Raw Sales files.'}
              {!hasSalesByProductFiles && ' Missing: Sales by Product files.'}
            </p>
          )}
        </div>
      )}

      {/* Confirmation Modal - Upload (redesigned content) */}
      <ConfirmationModal
        isOpen={showConfirmation}
        title="Review and Confirm Upload"
        message={
          <div className={styles.dataUploadConfirmContainer}>
            {/* Summary Stats */}
            <div className={styles.dataUploadConfirmSummary}>
              <div className={styles.dataUploadStat}>
                <div className={styles.dataUploadStatLabel}>Total Files</div>
                <div className={styles.dataUploadStatValue}>{allFiles.length}</div>
              </div>
              <div className={styles.dataUploadStat}>
                <div className={styles.dataUploadStatLabel}>Total Size</div>
                <div className={styles.dataUploadStatValue}>{totalSizeFormatted}</div>
              </div>
              <div className={styles.dataUploadStat}>
                <div className={styles.dataUploadStatLabel}>Categories</div>
                <div className={styles.dataUploadStatValue}>
                  Raw Sales: {rawSalesFiles.length} • Product: {salesByProductFiles.length}
                </div>
              </div>
            </div>

            <div className={styles.dataUploadDivider} />

            {/* Files by Category */}
            <div className={styles.dataUploadConfirmCategories}>
              {/* Raw Sales Files */}
              <div className={styles.dataUploadConfirmCategory}>
                <h4 className={styles.dataUploadCategoryTitle}>
                  Raw Sales Files ({rawSalesFiles.length})
                </h4>
                <div className={styles.dataUploadFileReviewList}>
                  {rawSalesFiles.length > 0 ? (
                    rawSalesFiles.map((file) => (
                      <div key={file.id} className={styles.dataUploadFileReviewItem}>
                        <div className={styles.dataUploadFileIcon}>
                          {getFileIcon(file.name)}
                        </div>
                        <div className={styles.dataUploadFileInfo}>
                          <span className={styles.dataUploadFileName} title={file.name}>
                            {getDisplayName(file.name)}
                          </span>
                          <span className={styles.dataUploadFileSize}>
                            {formatBytes(file.size)}
                          </span>
                        </div>
                        <span className={styles.dataUploadFileType}>
                          {getExtension(file.name).toUpperCase()}
                        </span>
                      </div>
                    ))
                  ) : (
                    <p className={styles.dataUploadEmptyCategory}>No files selected</p>
                  )}
                </div>
              </div>

              {/* Sales by Product Files */}
              <div className={styles.dataUploadConfirmCategory}>
                <h4 className={styles.dataUploadCategoryTitle}>
                  Sales by Product Files ({salesByProductFiles.length})
                </h4>
                <div className={styles.dataUploadFileReviewList}>
                  {salesByProductFiles.length > 0 ? (
                    salesByProductFiles.map((file) => (
                      <div key={file.id} className={styles.dataUploadFileReviewItem}>
                        <div className={styles.dataUploadFileIcon}>
                          {getFileIcon(file.name)}
                        </div>
                        <div className={styles.dataUploadFileInfo}>
                          <span className={styles.dataUploadFileName} title={file.name}>
                            {getDisplayName(file.name)}
                          </span>
                          <span className={styles.dataUploadFileSize}>
                            {formatBytes(file.size)}
                          </span>
                        </div>
                        <span className={styles.dataUploadFileType}>
                          {getExtension(file.name).toUpperCase()}
                        </span>
                      </div>
                    ))
                  ) : (
                    <p className={styles.dataUploadEmptyCategory}>No files selected</p>
                  )}
                </div>
              </div>
            </div>

            <div className={styles.dataUploadWarning}>
              <p>
                <MdWarning style={{ marginRight: '8px', verticalAlign: 'middle' }} />
                Please verify all file names, sizes, and categories before uploading. This action cannot be undone.
              </p>
            </div>
          </div>
        }
        confirmText={isUploading ? 'Uploading...' : 'Confirm Upload'}
        onConfirm={handleUpload}
        onCancel={() => setShowConfirmation(false)}
        isLoading={isUploading}
        variant="warning"
      />

      {/* Confirmation Modal - Remove Selected */}
      <ConfirmationModal
        isOpen={showRemoveSelectedConfirm}
        title="Remove selected files?"
        message={
          <div>
            <p>You are about to remove {selectedRawSalesIds.size + selectedSalesByProductIds.size} file(s)</p>
          </div>
        }
        confirmText="Remove"
        onConfirm={confirmRemoveSelected}
        onCancel={() => setShowRemoveSelectedConfirm(false)}
        isLoading={false}
        variant="danger"
      />

      {/* Confirmation Modal - Clear All / Remove Selected */}
      <ConfirmationModal
        isOpen={showClearAllConfirm}
        title={
          categoryToClear === 'raw_sales' && selectedRawSalesIds.size > 0
            ? `Remove ${selectedRawSalesIds.size} selected file(s)?`
            : categoryToClear === 'sales_by_product' && selectedSalesByProductIds.size > 0
            ? `Remove ${selectedSalesByProductIds.size} selected file(s)?`
            : 'Clear all files?'
        }
        message={
          categoryToClear === 'raw_sales' && selectedRawSalesIds.size > 0
            ? <p>You are about to remove {selectedRawSalesIds.size} selected file(s) from Raw Sales. This action cannot be undone.</p>
            : categoryToClear === 'sales_by_product' && selectedSalesByProductIds.size > 0
            ? <p>You are about to remove {selectedSalesByProductIds.size} selected file(s) from Sales by Product. This action cannot be undone.</p>
            : <p>This will remove all files from this category. This action cannot be undone.</p>
        }
        confirmText={
          (categoryToClear === 'raw_sales' && selectedRawSalesIds.size > 0) ||
          (categoryToClear === 'sales_by_product' && selectedSalesByProductIds.size > 0)
            ? 'Remove Selected'
            : 'Clear All'
        }
        onConfirm={confirmClearAll}
        onCancel={() => setShowClearAllConfirm(false)}
        isLoading={false}
        variant="danger"
      />

      {/* Confirmation Modal - Single Remove */}
      <ConfirmationModal
        isOpen={!!filePendingRemoval}
        title="Remove file?"
        message={
          filePendingRemoval ? (
            <div className={styles.dataUploadFileReviewList}>
              <div className={styles.dataUploadFileReviewItem}>
                <div className={styles.dataUploadFileInfo}>
                  <span className={styles.dataUploadFileName} title={filePendingRemoval.file.name}>
                    {getDisplayName(filePendingRemoval.file.name)}
                  </span>
                  <span className={styles.dataUploadFileSize}>
                    {formatBytes(filePendingRemoval.file.size)}
                  </span>
                </div>
                <span className={styles.dataUploadFileType}>
                  {getExtension(filePendingRemoval.file.name).toUpperCase()}
                </span>
              </div>
            </div>
          ) : null
        }
        confirmText="Remove"
        onConfirm={confirmRemoveSingle}
        onCancel={() => setFilePendingRemoval(null)}
        isLoading={false}
        variant="danger"
      />
    </div>
  );
};

export default DataUpload;