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
const MAX_DELETE_RETRIES = 10; // [NEW] 删除重试次数上限

class FseManager {
    constructor(options, logger) {
        this.logger = logger;
        this.options = { /* ... */ };
        this.simDataRoot = path.resolve(options.root || './');
        this.allTestRecordsPath = path.join(this.simDataRoot, 'AllTestRecords');
        this.records = new Map();
        this.wsServer = null;
        this.downloadQueue = new Map();
        this.activeDownloads = 0;
        this.deleteQueue = []; // 队列不变，依然是内存数组
        this.syncIntervalId = null;
        this.logger.info(chalk.yellow('FSE Manager initialized.'));
    }

    async start(httpServer) { /* ... no changes ... */ }

    stop() { /* ... no changes ... */ }

    async scanLocalRecordsAndResume() { /* ... no changes ... */ }

    // [MODIFIED] 增加了新的删除触发逻辑
    async syncWithP3D() {
        this.logger.info('Syncing with P3D server...');
        try {
            const response = await axios.post(this.options.p3dServerUrl, { jsonrpc: '2.0', method: 'getTestResults', params: [], id: Date.now() }, { proxy: false });
            if (response.data.error) throw new Error(`P3D RPC Error: ${response.data.error.message}`);
            const p3dRecords = response.data.result || [];
            this.logger.info(`P3D server returned ${p3dRecords.length} records.`);

            for (const p3dRecord of p3dRecords) {
                const recordId = p3dRecord.id;
                if (!recordId) continue;

                // [NEW] 如果本地已存在且已完成，则加入删除队列
                if (this.records.has(recordId)) {
                    const localRecord = this.records.get(recordId);
                    if (localRecord.State === eFSETestRecordState.eTRSOk) {
                        this.enqueueP3DDelete(recordId);
                    }
                    continue; // 忽略，不进行后续处理
                }

                const finalDirPath = path.join(this.allTestRecordsPath, recordId);
                try {
                    await fs.access(finalDirPath);
                    // ... (孤儿记录处理逻辑不变) ...
                } catch (error) {
                    if (error.code === 'ENOENT') {
                        // ... (创建新任务逻辑不变) ...
                    }
                }
            }
        } catch (error) { this.logger.info(chalk.red('Failed to sync with P3D server:'), error.message); }
    }

    async createNewDownloadTask(p3dRecord) { /* ... no changes ... */ }

    enqueueDownload(record) { /* ... no changes ... */ }

    processDownloadQueue() { /* ... no changes ... */ }

    async downloadRecordFiles(record) {
        // [MODIFIED] 完成后，只调用 enqueueP3DDelete，不再自己处理
        // ... (内部下载逻辑不变) ...
        const allFilesOk = Object.values(record.RecordFiles).every(f => f.State === eFSETestFileState.eTFSOk);
        if (allFilesOk) {
            // ... (写入和重命名逻辑不变) ...
            try {
                record.State = eFSETestRecordState.eTRSOk;
                await this.updateRecordFile(record);
                await fs.rename(tempDir, finalDir);

                this.logger.info(chalk.greenBright(`Record ${recordId} completed and finalized.`));
                this.wsServer.broadcast({ event: 'recordAdded', data: record });
                // 只需调用入队函数
                this.enqueueP3DDelete(recordId);
            } catch(e) { /* ... */ }
        }
    }

    async downloadFileWithRetry(record, fileType, attempt) { /* ... no changes ... */ }
    async updateRecordInfoFromMetadata(record, metadataBuffer) { /* ... no changes ... */ }

    // [MODIFIED] 统一的入队函数，防止重复添加
    enqueueP3DDelete(recordId) {
        // 检查是否已在队列中
        if (!this.deleteQueue.some(item => item.id === recordId)) {
            this.logger.info(chalk.magenta(`  - Queuing record ${recordId} for deletion from P3D server.`));
            this.deleteQueue.push({ id: recordId, retries: 0 });
            this.processDeleteQueue(); // 尝试立即处理
        }
    }
    
    // [MODIFIED] 完整实现了重试和失败处理逻辑
    async processDeleteQueue() {
        if (this.deleteQueue.length === 0) return;
        const item = this.deleteQueue.shift(); // 取出任务

        try {
            const response = await axios.post(this.options.p3dServerUrl, {
                jsonrpc: '2.0', method: 'deleteTestResults', params: [[item.id]], id: Date.now()
            }, { proxy: false });

            const result = response.data.result && response.data.result[0];
            
            // [NEW] 检查 `deleted` 状态
            if (result && result.deleted === true) {
                this.logger.info(chalk.green(`  - Record ${item.id} successfully deleted from P3D server.`));
            } else {
                // 如果 deleted 为 false 或结果格式不符，则视为删除失败
                throw new Error(result ? result.message : 'Deletion failed on server');
            }
        } catch (error) {
            this.logger.info(chalk.yellow(`  - Failed to delete record ${item.id} from P3D: ${error.message}`));
            item.retries++;
            // [NEW] 检查重试次数
            if (item.retries < MAX_DELETE_RETRIES) {
                this.logger.info(chalk.yellow(`    - Re-queuing task. Attempt ${item.retries + 1}/${MAX_DELETE_RETRIES}.`));
                this.deleteQueue.push(item); // 重新入队
            } else {
                this.logger.info(chalk.red(`  - Max retries reached for deleting ${item.id}. Giving up.`));
            }
        }
    }

    convertP3DRecordToLocal(p3dRecord) { /* ... no changes ... */ }
    async updateRecordFile(record) { /* ... no changes ... */ }
    getAllRecords() { return Array.from(this.records.values()); }
    async forceSyncWithP3D() { await this.syncWithP3D(); return true; }

    // constructor, start, stop, etc. are not fully shown for brevity but are unchanged
    // ... all other functions from the last correct version are here ...
    constructor(options, logger) {
        this.logger = logger;
        this.options = { p3dServerUrl: options.p3dServerUrl, syncInterval: options.syncInterval || 30000, downloadConcurrentLimit: options.downloadConcurrentLimit || 3, maxRetries: options.maxRetries || 5 };
        this.simDataRoot = path.resolve(options.root || './');
        this.allTestRecordsPath = path.join(this.simDataRoot, 'AllTestRecords');
        this.records = new Map();
        this.wsServer = null;
        this.downloadQueue = new Map();
        this.activeDownloads = 0;
        this.deleteQueue = [];
        this.syncIntervalId = null;
        this.logger.info(chalk.yellow('FSE Manager initialized.'));
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
                    const record = JSON.parse(await fs.readFile(indexPath, 'utf-8'));
                    if (record.RecordInfo && record.RecordInfo.ID) {
                        this.records.set(record.RecordInfo.ID, record);
                        // 最优恢复逻辑：精确的状态判断 + 临时目录判断
                        if ((record.State === eFSETestRecordState.eTRSDownloading ||
                             record.State === eFSETestRecordState.eTRSFileError ||
                             record.State === eFSETestRecordState.eTRSMissingFile) &&
                             entry.name.startsWith('__')) {
                            this.logger.info(chalk.cyan(`  - Resuming downloads for incomplete record: ${record.RecordInfo.ID}`));
                            this.enqueueDownload(record);
                        }
                    }
                } catch (err) { /* Ignore */ }
            }
            this.logger.info(`Scan complete. Restored ${this.records.size} records from disk.`);
        } catch (error) { this.logger.info(chalk.red('Error scanning local records:'), error); }
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
                    this.logger.info(chalk.red(`  - CRITICAL ERROR: metadata.json failed for ${recordId}. Aborting download for this record.`));
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
                // 正确的原子化提交流程
                record.State = eFSETestRecordState.eTRSOk;
                await this.updateRecordFile(record); // 将最终状态写入临时目录
                await fs.rename(tempDir, finalDir);   // 原子地重命名目录

                this.logger.info(chalk.greenBright(`Record ${recordId} completed and finalized.`));
                this.wsServer.broadcast({ event: 'recordAdded', data: record });
                this.enqueueP3DDelete(recordId);
            } catch (e) {
                if (e.code === 'EPERM' || e.code === 'EEXIST') {
                    this.logger.info(chalk.yellow(`Record ${recordId} appears to be already finalized.`));
                    record.State = eFSETestRecordState.eTRSOk;
                    this.wsServer.broadcast({ event: 'recordAdded', data: record });
                    this.enqueueP3DDelete(recordId);
                } else {
                    this.logger.error(chalk.red(`Failed to finalize record ${recordId}:`), e);
                }
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
            this.logger.info(chalk.blue(`  - Parsed metadata.json for ${record.RecordInfo.ID} and updated record details.`));
        } catch (e) {
            this.logger.error(chalk.red(`  - CRITICAL ERROR: Failed to parse downloaded metadata.json for ${record.RecordInfo.ID}.`));
            const metafile = Object.values(record.RecordFiles).find(f => f.FileName === METADATA_FILENAME);
            if (metafile) metafile.State = eFSETestFileState.eTFSError;
            record.State = eFSETestRecordState.eTRSFileError;
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
        const targetDir = path.join(this.allTestRecordsPath, `__${recordId}`);
        
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

}

module.exports = FseManager;