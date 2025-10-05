// http-server/lib/fse-manager.js

'use strict';

const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const FseWsServer = require('./fse-ws');
const chalk = require('chalk');

const eFSETestRecordState = { eTRSUnknown: 0, eTRSOk: 1, eTRSMissingFile: 2, eTRSFileError: 3, eTRSDownloading: 4, eTRSIsTesting: 5, eTRSIsPlayback: 6 };
const eFSETestFileState = { eTFSUnknown: 0, eTFSOk: 1, eTFSMissing: 2, eTFSError: 3, eTFSDownloading: 4 };
const P3D_API_TO_FILE_TYPE_MAP = { 'csv': 'eTFTTestRecordCSV', 'acmi': 'eTFTACMI', 'fsr': 'eTFTFSR', 'fxml': 'eTFTFXML', 'wx': 'eTFTWX', 'xml': 'eTFTP3DTaskXML', 'sav': 'eTFTFSESav', 'savXml': 'eTFTFSESavxml', 'metadata': 'eTFTMetadata' };
const LOCAL_INDEX_FILENAME = 'e9397340-a0ef-4c13-b3ac-d61d3298816f.json';
const METADATA_FILENAME = 'metadata.json';

class FseManager {
    constructor(options, logger) {
        this.logger = logger;
        this.options = {
            p3dGetResultsUrl: options.fse.p3dGetResultsUrl,
            p3dDeleteResultsUrl: options.fse.p3dDeleteResultsUrl,
            syncInterval: options.fse.syncInterval || 30000, 
            downloadConcurrentLimit: options.fse.downloadConcurrentLimit || 3, 
            maxRetries: options.fse.maxRetries || 5,
            deleteInterval: options.fse.deleteInterval || options.fse.syncInterval || 30000,
            deleteDelayHours: options.fse.deleteDelayHours || 24,
            root: options.root 
        };
        this.simDataRoot = path.resolve(this.options.root || './');
        this.allTestRecordsPath = path.join(this.simDataRoot, 'AllTestRecords');
        this.records = new Map();
        this.wsServer = null;

        this.p3dGetResultsUrl = this.options.p3dGetResultsUrl;
        this.p3dDeleteResultsUrl = this.options.p3dDeleteResultsUrl;

        this.downloadQueue = new Map();
        this.activeDownloads = 0;
        this.deleteQueue = [];
        this.syncIntervalId = null;
        this.deleteIntervalId = null;
        this.deleteDelayMs = this.options.deleteDelayHours * 60 * 60 * 1000;

        this.logger.info(chalk.yellow('FSE Manager initialized.'));
        if (this.p3dGetResultsUrl) {
            this.logger.info(chalk.cyan(`  - P3D GetResults Endpoint: ${this.p3dGetResultsUrl}`));
        } else {
            this.logger.info(chalk.red('  - P3D GetResults URL is not configured. Sync feature will be disabled.'));
        }
        if (this.p3dDeleteResultsUrl) {
            this.logger.info(chalk.cyan(`  - P3D DeleteResults Endpoint: ${this.p3dDeleteResultsUrl}`));
        } else {
            this.logger.info(chalk.yellow('  - P3D DeleteResults URL is not configured. Deletion feature will be disabled.'));
        }
        this.logger.info(chalk.cyan(`  - Sync Interval: ${this.options.syncInterval}ms`));
        this.logger.info(chalk.cyan(`  - Delete Interval: ${this.options.deleteInterval}ms`));
        this.logger.info(chalk.cyan(`  - Delete Delay: ${this.options.deleteDelayHours} hours`));
    }

    async start(httpServer) {
        await fs.mkdir(this.allTestRecordsPath, { recursive: true });
        this.wsServer = new FseWsServer(httpServer, this, this.logger);
        this.wsServer.start();

        if (this.p3dGetResultsUrl) {
            this.syncWithP3D();
            this.syncIntervalId = setInterval(() => this.syncWithP3D(), this.options.syncInterval);
        }
        if (this.p3dDeleteResultsUrl) {
            this.deleteIntervalId = setInterval(() => this.processDeleteQueue(), this.options.deleteInterval);
        }

        await this.scanLocalRecordsAndResume();
    }

    stop() {
        if (this.syncIntervalId) clearInterval(this.syncIntervalId);
        if (this.deleteIntervalId) clearInterval(this.deleteIntervalId);
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
                    const record = JSON.parse(await fs.readFile(indexPath, 'utf-8'));
                    if (record.RecordInfo && record.RecordInfo.ID) {
                        this.records.set(record.RecordInfo.ID, record);
                        if ((record.State === eFSETestRecordState.eTRSDownloading ||
                             record.State === eFSETestRecordState.eTRSFileError ||
                             record.State === eFSETestRecordState.eTRSMissingFile) &&
                             entry.name.startsWith('__')) {
                            this.logger.info(chalk.cyan(`  - Resuming downloads for incomplete record: ${record.RecordInfo.ID}`));
                            this.enqueueDownload(record);
                        }
                    }
                } catch (err) {
                    this.logger.info(chalk.red(`  - Failed to parse local index file at ${indexPath}. Error: ${err.message}. Skipping this record.`));
                }
            }
            this.logger.info(`Scan complete. Restored ${this.records.size} records from disk.`);
        } catch (error) { this.logger.info(chalk.red('Error scanning local records:'), error); }
    }

    async syncWithP3D() {
        if (!this.p3dGetResultsUrl) return;
        this.logger.info('Syncing with P3D server via HTTP...');
        try {
            const response = await axios.post(this.p3dGetResultsUrl);
            const p3dRecords = response.data;
            
            this.logger.info(`P3D server returned ${p3dRecords.length} records.`);
            for (const p3dRecord of p3dRecords) {
                const recordId = p3dRecord.id;
                if (!recordId) continue;

                if (this.records.has(recordId)) {
                    const localRecord = this.records.get(recordId);
                    if (localRecord.State === eFSETestRecordState.eTRSOk) {
                        this.enqueueP3DDelete(recordId);
                    }
                    continue;
                }

                const finalDirPath = path.join(this.allTestRecordsPath, recordId);
                try {
                    await fs.access(finalDirPath);
                    this.logger.info(chalk.yellow(`  - Found existing directory for record ${recordId}. Loading it into memory.`));
                    const indexPath = path.join(finalDirPath, LOCAL_INDEX_FILENAME);
                    const record = JSON.parse(await fs.readFile(indexPath, 'utf-8'));
                    this.records.set(recordId, record);
                } catch (error) {
                    if (error.code === 'ENOENT') {
                        this.logger.info(chalk.green(`  - Found new record: ${recordId}. Creating local task.`));
                        await this.createNewDownloadTask(p3dRecord);
                    }
                }
            }
        } catch (error) { this.logger.info(chalk.red('Failed to sync with P3D server:'), error.message); }
    }

    async createNewDownloadTask(p3dRecord) {
        if (!p3dRecord.metadata || !p3dRecord.metadata.url) {
            this.logger.info(chalk.red(`  - Invalid record from P3D (ID: ${p3dRecord.id}): Missing metadata. Record ignored.`));
            return;
        }
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
        if (this.downloadQueue.has(recordId)) return;
        this.downloadQueue.set(recordId, record);
        this.processDownloadQueue();
    }

    processDownloadQueue() {
        if (this.activeDownloads >= this.options.downloadConcurrentLimit || this.downloadQueue.size === 0) return;
        this.activeDownloads++;
        const [recordId, recordToProcess] = this.downloadQueue.entries().next().value;
        this.downloadQueue.delete(recordId);
        this.downloadRecordFiles(recordToProcess)
            .catch(error => this.logger.info(chalk.red(`Unhandled error in download process for ${recordId}:`), error))
            .finally(() => { this.activeDownloads--; this.processDownloadQueue(); });
    }

    async downloadRecordFiles(record) {
        const recordId = record.RecordInfo.ID;
        for (const fileType in record.RecordFiles) {
            const fileInfo = record.RecordFiles[fileType];
            if (fileInfo.State !== eFSETestFileState.eTFSOk) {
                const success = await this.downloadFileWithRetry(record, fileType, 0);
                if (!success && fileInfo.FileName === METADATA_FILENAME) {
                    this.logger.info(chalk.red(`  - CRITICAL ERROR: metadata.json failed for ${recordId}. Aborting download.`));
                    record.State = eFSETestRecordState.eTRSFileError;
                    await this.updateRecordFile(record);
                    return;
                }
            }
        }

        const allFilesOk = Object.values(record.RecordFiles).every(f => f.State === eFSETestFileState.eTFSOk);
        if (allFilesOk) {
            const tempDir = path.join(this.allTestRecordsPath, `__${recordId}`);
            const finalDir = path.join(this.allTestRecordsPath, recordId);
            try {
                record.State = eFSETestRecordState.eTRSOk;
                await this.updateRecordFile(record);
                await fs.rename(tempDir, finalDir);
                this.logger.info(chalk.greenBright(`Record ${recordId} completed and finalized.`));
                this.wsServer.broadcast({ event: 'recordAdded', data: record });
                this.enqueueP3DDelete(recordId);
            } catch (e) {
                if (e.code === 'EPERM' || e.code === 'EEXIST') {
                    this.logger.info(chalk.yellow(`Record ${recordId} already finalized.`));
                    record.State = eFSETestRecordState.eTRSOk;
                    this.wsServer.broadcast({ event: 'recordAdded', data: record });
                    this.enqueueP3DDelete(recordId);
                } else { this.logger.error(chalk.red(`Failed to finalize record ${recordId}:`), e); }
            }
        }
    }

    async downloadFileWithRetry(record, fileType, attempt) {
        const fileInfo = record.RecordFiles[fileType];
        if (attempt >= this.options.maxRetries) {
            fileInfo.State = eFSETestFileState.eTFSError;
            await this.updateRecordFile(record);
            return false;
        }
        const tempDir = path.join(this.allTestRecordsPath, `__${record.RecordInfo.ID}`);
        const localPath = path.join(tempDir, fileInfo.FileName);
        try {
            const response = await axios.get(fileInfo.Url, { responseType: 'arraybuffer', proxy: false });
            const fileData = Buffer.from(response.data);
            if (crypto.createHash('md5').update(fileData).digest('hex').toUpperCase() !== fileInfo.Hash.toUpperCase()) throw new Error(`MD5 mismatch`);
            await fs.writeFile(localPath, fileData);
            await fs.writeFile(`${localPath}.MD5`, fileInfo.Hash);
            fileInfo.State = eFSETestFileState.eTFSOk;
            if (fileInfo.FileName === METADATA_FILENAME) {
                await this.updateRecordInfoFromMetadata(record, fileData);
            }
            await this.updateRecordFile(record);
            this.logger.info(chalk.green(`    - Downloaded ${fileInfo.FileName}`));
            return true;
        } catch (error) {
            this.logger.error(`    - Attempt ${attempt + 1} for ${fileInfo.FileName} failed: ${error.message}`);
            await new Promise(resolve => setTimeout(resolve, 2000));
            return await this.downloadFileWithRetry(record, fileType, attempt + 1);
        }
    }

    async updateRecordInfoFromMetadata(record, metadataBuffer) {
        try {
            const metadataContent = JSON.parse(metadataBuffer.toString('utf-8'));
            Object.assign(record.RecordInfo, metadataContent);
            this.logger.info(chalk.blue(`  - Parsed metadata.json for ${record.RecordInfo.ID}.`));
        } catch (e) {
            this.logger.error(chalk.red(`  - CRITICAL ERROR: Failed to parse metadata.json for ${record.RecordInfo.ID}.`));
            const metafile = Object.values(record.RecordFiles).find(f => f.FileName === METADATA_FILENAME);
            if (metafile) metafile.State = eFSETestFileState.eTFSError;
            record.State = eFSETestRecordState.eTRSFileError;
        }
    }

    enqueueP3DDelete(recordId) {
        if (!this.p3dDeleteResultsUrl) return;
        if (!this.deleteQueue.some(item => item.id === recordId)) {
            this.logger.info(chalk.magenta(`  - Queuing record ${recordId} for delayed deletion from P3D.`));
            this.deleteQueue.push({ id: recordId, enqueueTime: Date.now() });
        }
    }

    async processDeleteQueue() {
        if (this.deleteQueue.length === 0 || !this.p3dDeleteResultsUrl) {
            return;
        }
        
        const now = Date.now();
        const itemsToDelete = this.deleteQueue.filter(item => (now - item.enqueueTime) > this.deleteDelayMs);

        if (itemsToDelete.length === 0) {
            return;
        }

        const idsToDelete = itemsToDelete.map(item => item.id);
        this.logger.info(chalk.yellow(`  - Attempting to delete ${idsToDelete.length} records from P3D that are older than ${this.options.deleteDelayHours} hours.`));

        try {
            const response = await axios.post(this.p3dDeleteResultsUrl, idsToDelete);
            const results = response.data;

            const successfullyDeletedIds = new Set();
            for (const result of results) {
                if (result.deleted) {
                    successfullyDeletedIds.add(result.id);
                } else {
                    this.logger.info(chalk.red(`  - Failed to delete record ${result.id} from P3D: ${result.message}`));
                }
            }
            
            if (successfullyDeletedIds.size > 0) {
                this.logger.info(chalk.green(`  - Successfully deleted ${successfullyDeletedIds.size} records from P3D.`));
                this.deleteQueue = this.deleteQueue.filter(item => !successfullyDeletedIds.has(item.id));
            }
            
        } catch (error) {
            this.logger.info(chalk.red(`  - Error sending delete request to P3D server: ${error.message}`));
        }
    }

    convertP3DRecordToLocal(p3dRecord) {
        const record = {
            Version: 1, State: eFSETestRecordState.eTRSUnknown,
            RecordInfo: { ID: p3dRecord.id },
            RecordFiles: {}, ParentFolderName: p3dRecord.id
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

    async updateRecordFile(record) {
        const recordId = record.RecordInfo.ID;
        const targetDir = record.State === eFSETestRecordState.eTRSOk 
            ? path.join(this.allTestRecordsPath, recordId)
            : path.join(this.allTestRecordsPath, `__${recordId}`);
        const indexPath = path.join(targetDir, LOCAL_INDEX_FILENAME);
        try {
            await fs.mkdir(targetDir, { recursive: true });
            await fs.writeFile(indexPath, JSON.stringify(record, null, 2));
            this.wsServer.broadcast({ event: 'recordStateChanged', data: { id: recordId, state: record.State, files: record.RecordFiles } });
        } catch (e) {
            this.logger.info(chalk.red(`Failed to write index for ${recordId} in ${targetDir}: ${e.message}`));
        }
    }

    getAllRecords() { return Array.from(this.records.values()); }
    async forceSyncWithP3D() { await this.syncWithP3D(); return true; }
    
    async validateRecordIntegrity(recordId) {
        const errors = [];
        const recordDir = path.join(this.allTestRecordsPath, recordId);

        try {
            await fs.access(recordDir);
        } catch (e) {
            errors.push(`Record directory not found at: ${recordDir}`);
            return { isValid: false, errors };
        }

        const indexPath = path.join(recordDir, LOCAL_INDEX_FILENAME);
        let indexData;
        try {
            indexData = JSON.parse(await fs.readFile(indexPath, 'utf-8'));
        } catch (e) {
            errors.push(`Failed to read or parse index file: ${indexPath}. Error: ${e.message}`);
            return { isValid: false, errors };
        }

        const metadataPath = path.join(recordDir, METADATA_FILENAME);
        let metadataJson;
        try {
            metadataJson = JSON.parse(await fs.readFile(metadataPath, 'utf-8'));
        } catch (e) {
            errors.push(`Failed to read or parse metadata file: ${metadataPath}. Error: ${e.message}`);
        }

        if (indexData && metadataJson) {
            if (indexData.RecordInfo.ID !== metadataJson.ID) {
                errors.push(`ID mismatch between index file ('${indexData.RecordInfo.ID}') and metadata.json ('${metadataJson.ID}').`);
            }
        }
        
        if (indexData && indexData.RecordFiles) {
            for (const fileType in indexData.RecordFiles) {
                const fileInfo = indexData.RecordFiles[fileType];
                const filePath = path.join(recordDir, fileInfo.FileName);
                
                try {
                    const fileData = await fs.readFile(filePath);
                    const hash = crypto.createHash('md5').update(fileData).digest('hex').toUpperCase();
                    if (hash !== fileInfo.Hash.toUpperCase()) {
                        errors.push(`MD5 mismatch for file '${fileInfo.FileName}'. Expected: ${fileInfo.Hash}, Calculated: ${hash}.`);
                    }
                } catch (e) {
                    if (e.code === 'ENOENT') {
                        errors.push(`File not found: '${fileInfo.FileName}' as listed in index file.`);
                    } else {
                        errors.push(`Could not read file '${fileInfo.FileName}'. Error: ${e.message}`);
                    }
                }
            }
        } else {
            errors.push('Index file does not contain a valid RecordFiles map.');
        }

        return { isValid: errors.length === 0, errors };
    }
}

module.exports = FseManager;