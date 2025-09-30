'use strict';

const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const FseWsServer = require('./fse-ws'); // 引入WebSocket服务模块
const chalk = require('chalk'); // <-- [FIX] 引入 chalk 库

// C++ 枚举值的JS映射
const eFSETestRecordState = {
    eTRSUnknown: 0,
    eTRSOk: 1,
    eTRSMissingFile: 2,
    eTRSFileError: 3,
    eTRSDownloading: 4,
    eTRSIsTesting: 5,
    eTRSIsPlayback: 6
};

const RECORD_INFO_FILENAME = 'e9397340-a0ef-4c13-b3ac-d61d3298816f.json';

class FseManager {
    constructor(options, logger) {
        this.logger = logger;
        this.options = {
            p3dServerUrl: options.p3dServerUrl,
            syncInterval: options.syncInterval || 30000,
            downloadConcurrentLimit: options.downloadConcurrentLimit || 3,
            maxRetries: options.maxRetries || 5,
        };
        
        // 确保服务根目录是绝对路径
        this.simDataRoot = path.resolve(options.root || './');
        this.allTestRecordsPath = path.join(this.simDataRoot, 'AllTestRecords');
        
        this.records = new Map(); // 内存中的测试记录缓存 <id, recordObject>
        this.wsServer = null; // WebSocket服务器实例
        
        this.downloadQueue = [];
        this.activeDownloads = 0;

        this.deleteQueue = [];
        
        this.syncIntervalId = null;

        this.logger.info(chalk.yellow('FSE Manager initialized.'));
        this.logger.info(chalk.yellow(`  - SimData Root: ${this.simDataRoot}`));
        this.logger.info(chalk.yellow(`  - P3D Server: ${this.options.p3dServerUrl}`));
    }
    
    // 启动管理器
    async start(httpServer) {
        // 1. 确保AllTestRecords目录存在
        await fs.mkdir(this.allTestRecordsPath, { recursive: true });

        // 2. 初始化并启动WebSocket服务器
        this.wsServer = new FseWsServer(httpServer, this, this.logger);
        this.wsServer.start();

        // 3. 首次扫描本地文件
        await this.scanLocalRecords();

        // 4. 首次与P3D服务器同步
        await this.syncWithP3D();

        // 5. 启动定时同步
        this.syncIntervalId = setInterval(
            () => this.syncWithP3D(),
            this.options.syncInterval
        );
    }
    
    // 停止管理器
    stop() {
        if (this.syncIntervalId) {
            clearInterval(this.syncIntervalId);
        }
        this.wsServer.stop();
        this.logger.info(chalk.yellow('FSE Manager stopped.'));
    }

    // -- 核心功能: 本地文件扫描 --
    async scanLocalRecords() {
        this.logger.info('Scanning local test records...');
        const newRecords = new Map();
        try {
            const entries = await fs.readdir(this.allTestRecordsPath, { withFileTypes: true });
            for (const entry of entries) {
                // 忽略文件和临时目录
                if (!entry.isDirectory() || entry.name.startsWith('__')) {
                    continue;
                }
                const recordId = entry.name;
                const jsonPath = path.join(this.allTestRecordsPath, recordId, RECORD_INFO_FILENAME);
                
                try {
                    const jsonData = await fs.readFile(jsonPath, 'utf-8');
                    const record = JSON.parse(jsonData);
                    
                    // 基本验证
                    if (record.RecordInfo && record.RecordInfo.ID === recordId) {
                         newRecords.set(recordId, record);
                    } else {
                        this.logger.info(chalk.red(`  - Ignoring invalid record in folder: ${recordId}`));
                    }
                } catch (err) {
                    // JSON文件读取或解析失败，忽略此目录
                    this.logger.info(chalk.red(`  - Ignoring directory, cannot read/parse info json: ${recordId}`));
                }
            }
            this.records = newRecords;
            this.logger.info(`Scan complete. Found ${this.records.size} valid local records.`);
        } catch (error) {
            this.logger.info(chalk.red('Error scanning local records:'), error);
        }
    }
    
    // -- 核心功能: 与P3D服务器同步 --
    async syncWithP3D() {
        this.logger.info('Syncing with P3D server...');
        try {
            const response = await axios.post(this.options.p3dServerUrl, {
                jsonrpc: '2.0',
                method: 'getTestResults',
                params: [],
                id: Date.now()
            });

            if (response.data.error) {
                throw new Error(`P3D RPC Error: ${response.data.error.message}`);
            }

            const p3dRecords = response.data.result || [];
            this.logger.info(`P3D server returned ${p3dRecords.length} records.`);

            for (const p3dRecord of p3dRecords) {
                if (!this.records.has(p3dRecord.id)) {
                    // 这是一个新记录，加入下载队列
                    this.logger.info(chalk.green(`  - Found new record: ${p3dRecord.id}. Enqueueing for download.`));
                    this.enqueueDownload(p3dRecord);
                }
            }

        } catch (error) {
            this.logger.info(chalk.red('Failed to sync with P3D server:'), error.message);
        }
    }

    // --- 下载管理器 ---
    enqueueDownload(p3dRecord) {
        // 防止重复入队
        if (this.downloadQueue.some(item => item.id === p3dRecord.id)) {
            return;
        }
        this.downloadQueue.push(p3dRecord);
        this.processDownloadQueue();
    }
    
    async processDownloadQueue() {
        if (this.activeDownloads >= this.options.downloadConcurrentLimit || this.downloadQueue.length === 0) {
            return;
        }

        this.activeDownloads++;
        const p3dRecord = this.downloadQueue.shift();
        
        try {
            await this.downloadRecord(p3dRecord);
        } catch (error) {
            this.logger.info(chalk.red(`Failed to download record ${p3dRecord.id}:`), error);
        } finally {
            this.activeDownloads--;
            this.processDownloadQueue(); // 处理下一个任务
        }
    }
    
    // 单个记录的完整下载流程
    async downloadRecord(p3dRecord) {
        const recordId = p3dRecord.id;
        const tempDir = path.join(this.allTestRecordsPath, `__${recordId}`);
        const finalDir = path.join(this.allTestRecordsPath, recordId);
        
        // 1. 创建临时目录
        await fs.mkdir(tempDir, { recursive: true });

        // 2. 根据P3D元数据创建本地记录对象
        const newRecord = this.createRecordObjectFromP3D(p3dRecord);
        newRecord.State = eFSETestRecordState.eTRSDownloading;
        
        // 3. 写入初始的json信息文件
        const jsonPath = path.join(tempDir, RECORD_INFO_FILENAME);
        await fs.writeFile(jsonPath, JSON.stringify(newRecord, null, 2));

        // 4. 搜集所有需要下载的文件
        const filesToDownload = [];
        for (const key in p3dRecord) {
            if (p3dRecord[key] && typeof p3dRecord[key] === 'object' && p3dRecord[key].url) {
                const fileInfo = p3dRecord[key];
                const localFileName = path.basename(new URL(fileInfo.url).pathname);
                filesToDownload.push({
                    url: fileInfo.url,
                    hash: fileInfo.hash,
                    localPath: path.join(tempDir, localFileName),
                    md5Path: path.join(tempDir, `${localFileName}.md5`)
                });
            }
        }
        
        // 5. 下载所有文件
        let allSucceeded = true;
        for (const file of filesToDownload) {
            const success = await this.downloadFileWithRetry(file.url, file.localPath, file.hash);
            if (!success) {
                allSucceeded = false;
                break;
            }
            // 写入md5文件
            await fs.writeFile(file.md5Path, file.hash);
        }

        // 6. 根据结果处理
        if (allSucceeded) {
            this.logger.info(chalk.green(`All files for record ${recordId} downloaded and verified successfully.`));
            // 更改状态为OK
            newRecord.State = eFSETestRecordState.eTRSOk;
            await fs.writeFile(jsonPath, JSON.stringify(newRecord, null, 2));

            // 重命名目录
            await fs.rename(tempDir, finalDir);
            
            // 更新内存缓存并通知Unreal
            this.records.set(recordId, newRecord);
            this.wsServer.broadcast({ event: 'recordAdded', data: newRecord });
            
            // 加入到P3D删除队列
            this.enqueueP3DDelete(recordId);

        } else {
            this.logger.info(chalk.red(`Failed to complete download for record ${recordId}. Cleaning up temp directory.`));
            // 更新状态为错误并通知 (可选)
            newRecord.State = eFSETestRecordState.eTRSFileError;
            this.wsServer.broadcast({ event: 'recordStateChanged', data: { id: recordId, state: newRecord.State } });
            // ... 可以选择不删除临时目录以供调试，这里我们选择删除
            await fs.rm(tempDir, { recursive: true, force: true });
        }
    }
    
    // 带重试和MD5校验的单个文件下载器
    async downloadFileWithRetry(url, localPath, expectedHash) {
        for (let i = 0; i < this.options.maxRetries; i++) {
            try {
                this.logger.info(`  - Attempt ${i+1}/${this.options.maxRetries}: Downloading ${url}`);
                const response = await axios.get(url, { responseType: 'arraybuffer' });
                const fileData = Buffer.from(response.data);
                
                // MD5校验
                const calculatedHash = crypto.createHash('md5').update(fileData).digest('hex').toUpperCase();
                if (calculatedHash !== expectedHash.toUpperCase()) {
                    throw new Error(`MD5 mismatch. Expected ${expectedHash}, got ${calculatedHash}`);
                }
                
                await fs.writeFile(localPath, fileData);
                return true; // 下载成功
            } catch (error) {
                this.logger.info(chalk.yellow(`    - Attempt ${i+1} failed: ${error.message}`));
                if (i === this.options.maxRetries - 1) {
                    return false; // 所有尝试都失败
                }
                await new Promise(resolve => setTimeout(resolve, 2000)); // 等待2秒后重试
            }
        }
        return false;
    }
    
    // --- P3D删除管理器 ---
    enqueueP3DDelete(recordId) {
        this.deleteQueue.push({ id: recordId, retries: 0 });
        this.processDeleteQueue(); // 立即尝试处理
    }
    
    async processDeleteQueue() {
        // 简单实现，可以改进为并发处理
        if (this.deleteQueue.length === 0) return;
        
        const item = this.deleteQueue.shift();
        
        try {
            this.logger.info(`Notifying P3D server to delete record ${item.id} (Attempt ${item.retries + 1})`);
            const response = await axios.post(this.options.p3dServerUrl, {
                jsonrpc: '2.0',
                method: 'deleteTestResults',
                params: [[item.id]],
                id: Date.now()
            });
            
            const result = response.data.result[0];
            if (!result || !result.deleted) {
                throw new Error(result ? result.message : 'Unknown error');
            }
            this.logger.info(chalk.green(`  - Record ${item.id} successfully deleted from P3D server.`));

        } catch (error) {
            this.logger.info(chalk.red(`  - Failed to delete record ${item.id} from P3D: ${error.message}`));
            item.retries++;
            if (item.retries < this.options.maxRetries) {
                this.deleteQueue.push(item); // 重新入队
            } else {
                this.logger.info(chalk.red(`  - Max retries reached for deleting ${item.id}. Giving up.`));
            }
        }
    }


    // --- 工具函数 ---
    createRecordObjectFromP3D(p3dRecord) {
        // 这个函数需要根据p3dRecord的`metadata`中的详细信息来填充
        // 目前我们先用p3dRecord的顶层信息来填充，您后续可以扩展
        const p3dMeta = p3dRecord.metadata; // 假设详细信息在metadata里
        
        const record = {
            Version: 1,
            State: eFSETestRecordState.eTRSUnknown,
            RecordInfo: {
                Version: 1,
                ID: p3dRecord.id,
                TotalT: p3dMeta ? p3dMeta.记录时长 : 0,
                MissionName: p3dMeta ? p3dMeta.任务名 : "Unknown",
                MissionID: p3dMeta ? p3dMeta.任务ID : "",
                HardwareBaseID: p3dMeta ? p3dMeta.底台ID : 0,
                HardwarePanelID: p3dMeta ? p3dMeta.面板ID : 0,
                PlaneID: p3dMeta ? p3dMeta.机型 : "Unknown",
                TestDate: new Date().toISOString()
            },
            RecordFiles: {}, // 这里需要根据p3dRecord的文件列表来填充
            ParentFolderName: p3dRecord.id
        };
        return record;
    }
    
    // --- unreal RPC调用的实现 ---
    getAllRecords() {
        return Array.from(this.records.values());
    }

    async forceSyncWithP3D() {
        await this.syncWithP3D();
        return true;
    }
    
    async updateRecordState(id, newState) {
        if (this.records.has(id)) {
            const record = this.records.get(id);
            record.State = newState;
            
            // 持久化到json文件
            const jsonPath = path.join(this.allTestRecordsPath, id, RECORD_INFO_FILENAME);
            try {
                await fs.writeFile(jsonPath, JSON.stringify(record, null, 2));
                this.logger.info(`State for record ${id} updated to ${newState} by Unreal client.`);
                
                // 广播状态变更
                this.wsServer.broadcast({
                    event: 'recordStateChanged',
                    data: { id, state: newState }
                });
                return true;
            } catch (error) {
                this.logger.info(chalk.red(`Failed to write updated state for record ${id}:`), error);
                return false;
            }
        }
        return false;
    }
}


module.exports = FseManager;