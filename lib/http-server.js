'use strict';

var fs = require('fs'),
  union = require('union'),
  httpServerCore = require('./core'),
  auth = require('basic-auth'),
  httpProxy = require('http-proxy'),
  corser = require('corser'),
  secureCompare = require('secure-compare');

const FseManager = require('./fse-manager');
const chalk = require('chalk');

//
// Remark: backwards compatibility for previous
// case convention of HTTP
//
exports.HttpServer = exports.HTTPServer = HttpServer;

/**
 * Returns a new instance of HttpServer with the
 * specified `options`.
 */
exports.createServer = function (options) {
  return new HttpServer(options);
};

/**
 * Constructor function for the HttpServer object
 * which is responsible for serving static files along
 * with other HTTP-related features.
 */
function HttpServer(options) {
  options = options || {};

  if (options.root) {
    this.root = options.root;
  } else {
    try {
      // eslint-disable-next-line no-sync
      fs.lstatSync('./public');
      this.root = './public';
    } catch (err) {
      this.root = './';
    }
  }

  this.headers = options.headers || {};
  this.headers['Accept-Ranges'] = 'bytes';

  this.cache = (
    // eslint-disable-next-line no-nested-ternary
    options.cache === undefined ? 3600 :
    // -1 is a special case to turn off caching.
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#Preventing_caching
      options.cache === -1 ? 'no-cache, no-store, must-revalidate' :
        options.cache // in seconds.
  );
  this.showDir = options.showDir !== 'false';
  this.autoIndex = options.autoIndex !== 'false';
  this.showDotfiles = options.showDotfiles;
  this.gzip = options.gzip === true;
  this.brotli = options.brotli === true;
  if (options.ext) {
    this.ext = options.ext === true
      ? 'html'
      : options.ext;
  }
  this.contentType = options.contentType ||
    this.ext === 'html' ? 'text/html' : 'application/octet-stream';

  this.fseManager = null;
  const logger = (options.logFn && options.logFn.info) ? options.logFn : { info: console.log, request: ()=>{} };

  if (options.fse) {
      this.fseManager = new FseManager({ ...options, ...options.fse }, logger);
  }


  var before = options.before ? options.before.slice() : [];

  // ============================================================
  // 中间件修改: 移除旧的RPC逻辑，只保留安全检查
  // ============================================================
  before.unshift(function (req, res) {
    // 检查对临时/私有目录的访问
    if (req.url.includes('/__')) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        res.end('Forbidden');
        return; // 停止处理链
    }
    
    // 所有其他请求都传递给下一个中间件
    res.emit('next');
  });

  if (options.logFn) {
    before.push(function (req, res) {
      // 修改日志记录器，以避免与WebSocket服务器的日志冲突
      if (req.url !== '/fse') {
          options.logFn(req, res);
      }
      res.emit('next');
    });
  }
  
  // ... (保留所有其他中间件逻辑: auth, cors, robots)
  if (options.username || options.password) {
    before.push(function (req, res) {
      var credentials = auth(req);

      if (credentials) {
        var usernameEqual = secureCompare(options.username.toString(), credentials.name);
        var passwordEqual = secureCompare(options.password.toString(), credentials.pass);
        if (usernameEqual && passwordEqual) {
          return res.emit('next');
        }
      }

      res.statusCode = 401;
      res.setHeader('WWW-Authenticate', 'Basic realm=""');
      res.end('Access denied');
    });
  }
  
  if (options.cors) {
    this.headers['Access-Control-Allow-Origin'] = '*';
    this.headers['Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept, Range';
    if (options.corsHeaders) {
      options.corsHeaders.split(/\s*,\s*/)
        .forEach(function (h) { this.headers['Access-Control-Allow-Headers'] += ', ' + h; }, this);
    }
    before.push(corser.create(options.corsHeaders ? {
      requestHeaders: this.headers['Access-Control-Allow-Headers'].split(/\s*,\s*/)
    } : null));
  }

  if (options.robots) {
    before.push(function (req, res) {
      if (req.url === '/robots.txt') {
        res.setHeader('Content-Type', 'text/plain');
        var robots = options.robots === true
          ? 'User-agent: *\nDisallow: /'
          : options.robots.replace(/\\n/, '\n');

        return res.end(robots);
      }

      res.emit('next');
    });
  }


  before.push(httpServerCore({
    root: this.root,
    baseDir: options.baseDir,
    cache: this.cache,
    showDir: this.showDir,
    showDotfiles: this.showDotfiles,
    autoIndex: this.autoIndex,
    defaultExt: this.ext,
    gzip: this.gzip,
    brotli: this.brotli,
    contentType: this.contentType,
    mimetypes: options.mimetypes,
    handleError: typeof options.proxy !== 'string'
  }));

  if (typeof options.proxy === 'string') {
    var proxyOptions = options.proxyOptions || {};
    var proxy = httpProxy.createProxyServer(proxyOptions);
    before.push(function (req, res) {
      proxy.web(req, res, {
        target: options.proxy,
        changeOrigin: true
      }, function (err, req, res) {
        if (options.logFn) {
          options.logFn(req, res, {
            message: err.message,
            status: res.statusCode });
        }
        res.emit('next');
      });
    });
  }

  var serverOptions = {
    before: before,
    headers: this.headers,
    onError: function (err, req, res) {
      if (options.logFn) {
        options.logFn(req, res, err);
      }

      res.end();
    }
  };

  if (options.https) {
    serverOptions.https = options.https;
  }

  this.server = serverOptions.https && serverOptions.https.passphrase
    ? require('./shims/https-server-shim')(serverOptions)
    : union.createServer(serverOptions);

  if (this.fseManager) {
      this.fseManager.start(this.server);
  }


  if (options.timeout !== undefined) {
    this.server.setTimeout(options.timeout);
  }

  if (typeof options.proxy === 'string') {
    this.server.on('upgrade', function (request, socket, head) {
      proxy.ws(request, socket, head, {
        target: options.proxy,
        changeOrigin: true
      }, function (err, req, res) {
        if (options.logFn) {
          options.logFn(req, res, {
            message: err?.message,
            status: res?.statusCode });
        }
        res.emit('next');
      });
    });
  }
}

HttpServer.prototype.listen = function () {
  this.server.listen.apply(this.server, arguments);
};

HttpServer.prototype.close = function () {
  // 新增: 优雅地关闭FSE管理器
  if (this.fseManager) {
      this.fseManager.stop();
  }
  return this.server.close();
};