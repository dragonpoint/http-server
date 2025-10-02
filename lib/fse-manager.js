'use strict';

const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const FseWsServer = require('./fse-ws');
const chalk = require('chalk');

// C++ 枚举值的JS映射 (与 FSEMission.h 保持完全一致)
const eFSETestRecordState = {
    eTRSUnknown: 0, eTRSOk: 1, eTRSMissingFile: 2, eTRSFileError: 3, 
    eTRSDownloading: 4, eTRSIsTesting: 5, eTRSIsPlayback: 6
};

const eFSETestFileState = {
    eTFSUnknown: 0, eTFSOk: 1, eTFSMissing: 2, eTFSError: 3, eTFSDownloading: 4
};

// P3D API 字段到 C++ eFSETestFileType 枚举的逆向映射
const P3D_API_TO_FILE_TYPE_MAP = {
    'csv': 'eTFTTestRecordCSV', 'acmi': 'eTFTACMI', 'fsr': 'eTFTFSR',
    'fxml': 'eTFTFXML', 'wx': 'eTFTWX', 'xml': 'eTFTP3DTaskXML',
    'sav': 'eTFTFSESav', 'savXml': 'eTFTFSESavxml', 'metadata': 'eTFTMetadata'
};

const LOCAL_INDEX_FILENAME = 'e9397340-a0ef-4c13-b3ac-d61d3298816f.json';

class FseManager {
    constructor(options, logger) {
        this.logger = logger;
        this.options = {
            p3dServerUrl: options.p3dServerUrl,
            syncInterval: options.syncInterval || 30000,
            downloadConcurrentLimit: options.downloadConcurrentLimit || 3,
            maxRetries: options.maxRetries || 5,
        };
        
        this.simDataRoot = path.resolve(options.root || './');
        this.allTestRecordsPath = path.join(this.simDataRoot, 'AllTestRecords');
        
        this.records = new Map();
        this.wsServer = null;
        this.downloadQueue = [];
        this.activeDownloads = 0;
        this.deleteQueue = [];
        this.syncIntervalId = null;

        this.logger.info(chalk.yellow('FSE Manager initialized.'));
        this.logger.info(chalk.yellow(`  - SimData Root: ${this.simDataRoot}`));
        this.logger.info(chalk.yellow(`  - P3D Server: ${this.options.p3dServerUrl}`));
    }
    
    async start(httpServer) {
        await fs.mkdir(this.allTestRecordsPath, { recursive: true });

        this.wsServer = new FseWsServer(httpServer, this, this.logger);
        this.wsServer.start();

        await this.scanLocalRecordsAndResume();
        await this.syncWithP3D();
        this.syncIntervalId = setInterval(() => this.syncWithP3D(), this.options.syncInterval);
    }
    
    stop() {
        if (this.syncIntervalId) clearInterval(this.syncIntervalId);
        this.wsServer.stop();
        this.logger.info(chalk.yellow('FSE Manager stopped.'));
    }

    async scanLocalRecordsAndResume() {
        this.logger.info('Scanning local records to restore state...');
        try {
            const entries = await fs.readdir(this.allTestRecordsPath, { withFileTypes: true });
            for (const entry of entries) {
                if (!entry.isDirectory()) continue;
                
                const recordDir = path.join(this.allTestRecordsPath, entry.name);
                const indexPath = path.join(recordDir, LOCAL_INDEX_FILENAME);
                
                try {
                    const jsonData = await fs.readFile(indexPath, 'utf-8');
                    const record = JSON.parse(jsonData);
                    
                    if (record.RecordInfo && record.RecordInfo.ID) {
                        this.records.set(record.RecordInfo.ID, record);
                        if (record.State !== eFSETestRecordState.eTRSOk && entry.name.startsWith('__')) {
                            this.logger.info(chalk.cyan(`  - Resuming downloads for incomplete record: ${record.RecordInfo.ID}`));
                            this.enqueueDownload(record);
                        }
                    }
                } catch (err) {
                    // Ignore directories without a valid index file
                }
            }
            this.logger.info(`Scan complete. Restored ${this.records.size} records from disk.`);
        } catch (error) {
            this.logger.info(chalk.red('Error scanning local records:'), error);
        }
    }
    
    async syncWithP3D() {
        this.logger.info('Syncing with P3D server...');
        try {
            const response = await axios.post(this.options.p3dServerUrl, {
                jsonrpc: '2.0', method: 'getTestResults', params: [], id: Date.now()
            }, { proxy: false });

            if (response.data.error) throw new Error(`P3D RPC Error: ${response.data.error.message}`);

            const p3dRecords = response.data.result || [];
            this.logger.info(`P3D server returned ${p3dRecords.length} records.`);

            for (const p3dRecord of p3dRecords) {
                if (!this.records.has(p3dRecord.id)) {
                    this.logger.info(chalk.green(`  - Found new record: ${p3dRecord.id}. Creating local task.`));
                    await this.createNewDownloadTask(p3dRecord);
                }
            }
        } catch (error) {
            this.logger.info(chalk.red('Failed to sync with P3D server:'), error.message);
        }
    }

    async createNewDownloadTask(p3dRecord) {
        const recordId = p3dRecord.id;
        const tempDir = path.join(this.allTestRecordsPath, `__${recordId}`);
        await fs.mkdir(tempDir, { recursive: true });

        const newRecord = this.convertP3DRecordToLocal(p3dRecord);
        newRecord.State = eFSETestRecordState.eTRSDownloading;

        await this.updateRecordFile(newRecord);
        
        this.records.set(recordId, newRecord);
        this.enqueueDownload(newRecord);
    }

    enqueueDownload(record) {
        const recordId = record.RecordInfo.ID;
        if (this.downloadQueue.some(item => item.RecordInfo.ID === recordId)) return;
        
        this.downloadQueue.push(record);
        this.processDownloadQueue();
    }
    
    processDownloadQueue() {
        if (this.activeDownloads >= this.options.downloadConcurrentLimit || this.downloadQueue.length === 0) return;

        this.activeDownloads++;
        const recordToProcess = this.downloadQueue.shift();
        
        this.downloadRecordFiles(recordToProcess)
            .catch(error => this.logger.info(chalk.red(`Unhandled error in download process for ${recordToProcess.RecordInfo.ID}:`), error))
            .finally(() => {
                this.activeDownloads--;
                this.processDownloadQueue();
            });
    }
    
    async downloadRecordFiles(record) {
        const recordId = record.RecordInfo.ID;
        
        const filesToDownload = Object.keys(record.RecordFiles).filter(
            fileType => record.RecordFiles[fileType].State !== eFSETestFileState.eTFSOk
        );

        for (const fileType of filesToDownload) {
            await this.downloadFileWithRetry(record, fileType, 0);
        }

        // Re-check completion status after all downloads attempted
        const isComplete = Object.values(record.RecordFiles).every(f => f.State === eFSETestFileState.eTFSOk);

        if (isComplete) {
            record.State = eFSETestRecordState.eTRSOk;
            await this.updateRecordFile(record);
            
            const tempDir = path.join(this.allTestRecordsPath, `__${recordId}`);
            const finalDir = path.join(this.allTestRecordsPath, recordId);
            try {
                await fs.rename(tempDir, finalDir);
                this.logger.info(chalk.greenBright(`Record ${recordId} completed and finalized.`));
                this.wsServer.broadcast({ event: 'recordAdded', data: record });
                this.enqueueP3DDelete(recordId);
            } catch (e) {
                if (e.code === 'EEXIST') { // If final dir already exists, consider it done
                    this.logger.info(chalk.yellow(`Record ${recordId} already finalized. Deleting temp dir.`));
                    await fs.rm(tempDir, { recursive: true, force: true });
                    this.enqueueP3DDelete(recordId);
                } else {
                    this.logger.info(chalk.red(`Failed to finalize record ${recordId}:`), e);
                }
            }
        } else {
            // If not complete, ensure the global state reflects an error
            record.State = eFSETestRecordState.eTRSFileError;
            await this.updateRecordFile(record);
        }
    }
    
    async downloadFileWithRetry(record, fileType, attempt) {
        const fileInfo = record.RecordFiles[fileType];
        if (attempt >= this.options.maxRetries) {
            fileInfo.State = eFSETestFileState.eTFSError;
            await this.updateRecordFile(record);
            this.logger.info(chalk.red(`  - Max retries reached for ${fileInfo.FileName}. Giving up.`));
            return false;
        }

        const tempDir = path.join(this.allTestRecordsPath, `__${record.RecordInfo.ID}`);
        const localPath = path.join(tempDir, fileInfo.FileName);
        
        try {
            this.logger.info(`  - Attempt ${attempt + 1}/${this.options.maxRetries}: Downloading ${fileInfo.Url}`);
            const response = await axios.get(fileInfo.Url, { responseType: 'arraybuffer', proxy: false });
            const fileData = Buffer.from(response.data);
            
            const calculatedHash = crypto.createHash('md5').update(fileData).digest('hex').toUpperCase();
            if (calculatedHash !== fileInfo.Hash.toUpperCase()) {
                throw new Error(`MD5 mismatch for ${fileInfo.FileName}`);
            }
            
            await fs.writeFile(localPath, fileData);
            
            // [NEW FEATURE] Create the corresponding .MD5 file
            const md5Path = localPath + '.MD5';
            await fs.writeFile(md5Path, fileInfo.Hash);

            fileInfo.State = eFSETestFileState.eTFSOk;
            await this.updateRecordFile(record);
            this.logger.info(chalk.green(`    - Successfully downloaded and verified ${fileInfo.FileName}`));
            
            return true;
        } catch (error) {
            this.logger.info(chalk.yellow(`    - Attempt ${attempt + 1} failed: ${error.message}`));
            await new Promise(resolve => setTimeout(resolve, 2000));
            return await this.downloadFileWithRetry(record, fileType, attempt + 1);
        }
    }

    enqueueP3DDelete(recordId) {
        this.deleteQueue.push({ id: recordId, retries: 0 });
        this.processDeleteQueue();
    }

    async processDeleteQueue() {
        if (this.deleteQueue.length === 0) return;
        const item = this.deleteQueue.shift();
        try {
            await axios.post(this.options.p3dServerUrl, {
                jsonrpc: '2.0', method: 'deleteTestResults', params: [[item.id]], id: Date.now()
            }, { proxy: false });
            this.logger.info(chalk.green(`  - Notified P3D to delete record ${item.id}.`));
        } catch (error) {
            item.retries++;
            if (item.retries < this.options.maxRetries) this.deleteQueue.push(item);
        }
    }

    convertP3DRecordToLocal(p3dRecord) {
        const record = {
            Version: 1,
            State: eFSETestRecordState.eTRSUnknown,
            RecordInfo: {
                Version: 1, ID: p3dRecord.id, TotalT: p3dRecord['记录时长'] || 0,
                MissionName: p3dRecord['任务名'] || "Unknown", MissionID: p3dRecord['任务ID'] || "",
                HardwareBaseID: p3dRecord['底台ID'] || 0, HardwarePanelID: p3dRecord['面板ID'] || 0,
                PlaneID: p3dRecord['机型'] || "Unknown", TestDate: new Date().toISOString()
            },
            RecordFiles: {},
            ParentFolderName: p3dRecord.id
        };

        for (const apiKey in P3D_API_TO_FILE_TYPE_MAP) {
            if (p3dRecord[apiKey]) {
                const fileTypeEnum = P3D_API_TO_FILE_TYPE_MAP[apiKey];
                const p3dFileInfo = p3dRecord[apiKey];
                record.RecordFiles[fileTypeEnum] = {
                    Version: 1, Url: p3dFileInfo.url, Hash: p3dFileInfo.hash,
                    State: eFSETestFileState.eTFSMissing,
                    FileName: path.basename(new URL(p3dFileInfo.url).pathname)
                };
            }
        }
        return record;
    }

    // [FIX] Corrected logic to determine the correct directory to write to
    async updateRecordFile(record) {
        const recordId = record.RecordInfo.ID;
        const tempDir = path.join(this.allTestRecordsPath, `__${recordId}`);
        const finalDir = path.join(this.allTestRecordsPath, recordId);
        let targetDir = tempDir; // Default to temp directory

        try {
            // Check if the temporary directory still exists. If not, it means it has been finalized.
            await fs.access(tempDir);
        } catch (error) {
            // If temp dir doesn't exist, switch to writing in the final directory.
            targetDir = finalDir;
        }
        
        const indexPath = path.join(targetDir, LOCAL_INDEX_FILENAME);
        try {
            // Ensure the target directory exists before writing
            await fs.mkdir(targetDir, { recursive: true });
            await fs.writeFile(indexPath, JSON.stringify(record, null, 2));
            this.wsServer.broadcast({ event: 'recordStateChanged', data: { id: recordId, state: record.State, files: record.RecordFiles } });
        } catch (e) {
            this.logger.info(chalk.red(`Failed to write index for ${recordId} in ${targetDir}: ${e.message}`));
        }
    }
    
    getAllRecords() {
        return Array.from(this.records.values());
    }

    async forceSyncWithP3D() {
        await this.syncWithP3D(); return true;
    }
}

module.exports = FseManager;
