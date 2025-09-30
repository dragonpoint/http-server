'use strict';

const { WebSocketServer } = require('ws');
const chalk = require('chalk');

class FseWsServer {
    constructor(httpServer, manager, logger) {
        this.httpServer = httpServer;
        this.manager = manager;
        this.logger = logger;
        this.wss = null;
    }
    
    start() {
        this.wss = new WebSocketServer({ server: this.httpServer });
        this.logger.info(chalk.yellow('FSE WebSocket Server started, waiting for connections...'));
        
        this.wss.on('connection', (ws) => {
            this.logger.info(chalk.cyan('Unreal client connected.'));
            
            ws.on('message', (message) => {
                this.handleMessage(ws, message);
            });
            
            ws.on('close', () => {
                this.logger.info(chalk.cyan('Unreal client disconnected.'));
            });

            ws.on('error', (error) => {
                this.logger.info(chalk.red('WebSocket error:'), error);
            });
        });
    }

    stop() {
        if (this.wss) {
            this.wss.close();
            this.logger.info(chalk.yellow('FSE WebSocket Server stopped.'));
        }
    }
    
    async handleMessage(ws, message) {
        let request;
        try {
            request = JSON.parse(message);
        } catch (e) {
            this.sendError(ws, null, -32700, 'Parse error');
            return;
        }

        const { jsonrpc, method, params, id } = request;

        if (jsonrpc !== '2.0' || !method) {
            this.sendError(ws, id, -32600, 'Invalid Request');
            return;
        }

        let result;
        try {
            switch (method) {
                case 'getAllRecords':
                    result = this.manager.getAllRecords();
                    break;
                case 'forceSyncWithP3D':
                    result = await this.manager.forceSyncWithP3D();
                    break;
                case 'updateRecordState':
                    if (!Array.isArray(params) || params.length !== 2) {
                        throw new Error('Invalid params for updateRecordState');
                    }
                    result = await this.manager.updateRecordState(params[0], params[1]);
                    break;
                default:
                    this.sendError(ws, id, -32601, 'Method not found');
                    return;
            }
            this.sendResult(ws, id, result);
        } catch (error) {
            this.sendError(ws, id, -32603, `Internal error: ${error.message}`);
        }
    }
    
    send(ws, data) {
        if (ws.readyState === 1) { // OPEN
            ws.send(JSON.stringify(data));
        }
    }
    
    sendResult(ws, id, result) {
        this.send(ws, { jsonrpc: '2.0', id, result });
    }

    sendError(ws, id, code, message) {
        this.send(ws, {
            jsonrpc: '2.0',
            id,
            error: { code, message }
        });
    }
    
    // 向所有连接的客户端广播消息
    broadcast(data) {
        this.logger.info(`Broadcasting event: ${data.event}`);
        const message = JSON.stringify({ jsonrpc: '2.0', method: 'event', params: data });
        this.wss.clients.forEach(client => {
            if (client.readyState === 1) { // OPEN
                client.send(message);
            }
        });
    }
}

module.exports = FseWsServer;