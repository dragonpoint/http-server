'use strict';

var fs = require('fs'),
  union = require('union'),
  httpServerCore = require('./core'),
  auth = require('basic-auth'),
  httpProxy = require('http-proxy'),
  corser = require('corser'),
  secureCompare = require('secure-compare');

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

  var before = options.before ? options.before.slice() : [];

  // ============================================================
  // THIS IS THE FINAL FIX: A SINGLE MIDDLEWARE AT THE VERY FRONT
  // That listens to the RAW request stream.
  // ============================================================
  before.unshift(function (req, res) {
    // First, check for blocked directory access
    if (req.url.includes('/__')) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
        return; // Stop the chain, request is handled.
    }

    // Next, check for RPC requests
    if (req.url === '/rpc' && req.method === 'POST') {
      
      let body = '';
      
      // CRITICAL FIX: Listen on the underlying, original request object (`req.request`),
      // not the `union` wrapper (`req`). This guarantees we get the data first.
      req.request.on('data', (chunk) => {
        body += chunk.toString();
      });

      req.request.on('end', () => {
        let rpcRequest;
        try {
          if (!body) {
            throw new Error("Empty body");
          }

          rpcRequest = JSON.parse(body);
          
          const { jsonrpc, method, params, id } = rpcRequest;

          if (jsonrpc !== '2.0' || !method) {
            throw new Error("Invalid Request");
          }

          let result = null;
          let error = null;

          switch (method) {
            case 'echo':
              result = params;
              if (options.logFn) {
                  // CRITICAL FIX for logging: Pass an object with the properties the logger expects.
                  options.logFn(
                    { method: 'RPC', url: 'echo', headers: req.request.headers },
                    { statusCode: 200 }
                  );
              }
              console.log('RPC \'echo\' called with params:', params);
              break;
            case 'executeCommand':
              if (options.logFn) {
                  // CRITICAL FIX for logging: Pass an object with the properties the logger expects.
                  options.logFn(
                    { method: 'RPC', url: 'executeCommand', headers: req.request.headers },
                    { statusCode: 200 }
                  );
              }
              console.log('Executing command:', params);
              result = { status: 'success', command: params.command, message: `Command '${params.command}' received successfully.` };
              break;
            default:
              error = { code: -32601, message: 'Method not found' };
              break;
          }

          const rpcResponse = { jsonrpc: '2.0', id: id };
          if (error) {
            rpcResponse.error = error;
          } else {
            rpcResponse.result = result;
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(rpcResponse));

        } catch (e) {
          let errorPayload;
          if (e.message === "Invalid Request") {
              errorPayload = { code: -32600, message: 'Invalid Request' };
          } else {
              errorPayload = { code: -32700, message: 'Parse error: Invalid or empty JSON body received.' };
          }
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0',
            error: errorPayload,
            id: (rpcRequest && rpcRequest.id) || null,
          }));
        }
      });
      return; // Stop the chain, request is being handled by us.
    }
    
    // If it's neither a blocked dir nor an RPC call, pass it on.
    res.emit('next');
  });

  if (options.logFn) {
    before.push(function (req, res) {
      options.logFn(req, res);
      res.emit('next');
    });
  }

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
  return this.server.close();
};

