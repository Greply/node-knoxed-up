    var fs          = require('fs');
    var mime        = require('mime');
    var path        = require('path');
    var util        = require('util');
    var Knox        = require('knox');
    var fsX         = require('fs-extended');
    var xml2js      = require('xml2js');
    var async       = require('async');
    var Buffer      = require('buffer').Buffer;
    var syslog      = require('syslog-console').init('KnoxedUp');
    var Emitter     = require('events').EventEmitter;

    var KnoxedUp = function(oConfig) {
        if (oConfig.AMAZON !== undefined) {
            this.oConfig = {
                key:    oConfig.AMAZON.SERVER.ID,
                secret: oConfig.AMAZON.SERVER.SECRET,
                bucket: oConfig.AMAZON.BUCKET,
                port:   80
            };

            this.sOriginalBucket = oConfig.AMAZON.BUCKET;

            if (oConfig.AMAZON.LOCAL       !== undefined
            &&  oConfig.AMAZON.LOCAL_PATH  !== undefined) {
                KnoxedUp.setLocal(oConfig.AMAZON.LOCAL, oConfig.AMAZON.LOCAL_PATH);
            }
        } else if (oConfig.key !== undefined) {
            this.oConfig         = oConfig;
            this.sOriginalBucket = oConfig.bucket;
        }

        this.Client  = Knox.createClient(this.oConfig);
    };

    /**
     *
     * @param {String} sCommand
     * @param {String} sFilename
     * @param {String} sType
     * @param {Object} oHeaders
     * @param {Function} fCallback
     * @param {Integer} [iRetries]
     * @private
     */
    KnoxedUp.prototype._command = function (sCommand, sFilename, sType, oHeaders, fCallback, iRetries) {
        var iRetries     = iRetries !== undefined ? iRetries : 0;
        var bHasCallback = typeof fCallback == 'function';

        var oLog = {
            action:  'KnoxedUp._command.' + sCommand,
            command:  sCommand,
            file:     sFilename,
            headers:  oHeaders,
            retries:  iRetries,
            callback: bHasCallback
        };

        var iStart = syslog.timeStart(oLog);
        var fDone  = function(fDoneCallback, oError, oResponse, sData) {
            if (oError) {
                syslog.error(oLog);
            } else {
                syslog.timeStop(iStart, oLog);
            }

            fDoneCallback(oError, oResponse, sData, iRetries);
        };

        var oRequest     = this.Client[sCommand](sFilename, oHeaders);
        var iLengthTotal = null;
        var iLength      = 0;
        var sData        = '';

        oRequest.on('error', function(oError) {
            if (oError.message == 'socket hang up') {
                if (iRetries > 3) {
                    oLog.action += '.request.hang_up.retry.max';
                    oLog.error   = oError;
                    fDone(fCallback, oLog.error);
                } else {
                    oLog.action += '.request.hang_up.retry';
                    syslog.warn(oLog);
                    this._command(sCommand, sFilename, sType, oHeaders, fCallback, iRetries + 1);
                }
            } else {
                oLog.action += '.request.error';
                oLog.error   = oError;
                fDone(fCallback, oLog.error);
            }
        }.bind(this));

        oRequest.on('response', function(oResponse) {
            oLog.status  = oResponse.statusCode;

            if (oResponse.headers !== undefined) {
                if (oResponse.headers['content-length'] !== undefined) {
                    iLengthTotal = parseInt(oResponse.headers['content-length'], 10);
                }
            }

            if (oResponse.statusCode == 500) {
                oLog.action += '.request.hang_up.retry';
                oLog.error = new Error('S3 Error Code ' + oResponse.statusCode);
                if (iRetries > 3) {
                    oLog.action += '.max';
                    fDone(fCallback, oLog.error);
                } else {
                    syslog.warn(oLog);
                    this._command(sCommand, sFilename, sType, oHeaders, fCallback, iRetries + 1);
                }
            } else if(oResponse.statusCode > 399) {
                switch(oResponse.statusCode) {
                    case 404:
                        oLog.error = new Error('File Not Found');
                        break;

                    default:
                        oLog.error = new Error('S3 Error Code ' + oResponse.statusCode);
                        break;
                }

                fDone(fCallback, oLog.error);
            } else {
                oResponse.setEncoding(sType);
                oResponse
                    .on('error', function(oError){
                        oLog.error = oError;
                        fDone(fCallback, oLog.error);
                    })
                    .on('data', function(sChunk){
                        sData   += sChunk;
                        iLength += sChunk.length;
                    })
                    .on('end', function(){
                        if (sCommand == 'get') {
                            if (iLengthTotal !== null) {
                                oLog.length = {
                                    download: iLength,
                                    total:    iLengthTotal
                                };

                                if (iLength < iLengthTotal) {
                                    oLog.error = new Error('Content Length did not match Header');
                                    return fDone(fCallback, oLog.error);
                                }
                            }
                        }

                        oLog.action += '.done';
                        fDone(fCallback, null, oResponse, sData);
                    });
            }
        }.bind(this));

        oRequest.end();

        return oRequest;
    };

    KnoxedUp.prototype._get = function (sFilename, sType, oHeaders, fCallback) {
        if (!sFilename.match(/^\//)) {
            sFilename = '/' + sFilename;
        }

        return this._command('get', sFilename, sType, oHeaders, fCallback);
    };

    KnoxedUp.prototype._put = function (sFilename, sType, oHeaders, fCallback) {
        return this._command('put', sFilename, sType, oHeaders, fCallback);
    };

    KnoxedUp.prototype._head = function (sFilename, oHeaders, fCallback) {
        return this._command('head', sFilename, 'utf-8', oHeaders, fCallback);
    };

    KnoxedUp.prototype._delete = function (sFilename, oHeaders, fCallback) {
        return this._command('del', sFilename, 'utf-8',oHeaders, fCallback);
    };

    KnoxedUp.prototype.getFile = function (sFilename, sToFile, sType, fCallback) {
        syslog.debug({action: 'KnoxedUp.getFile', file: sFilename, to: sToFile, type: sType});

        var iStart  = syslog.timeStart();
        var bError  = false;
        var bClosed = false;
        var oToFile = fs.createWriteStream(sToFile, {
            flags:    'w',
            encoding: sType
        });

        oToFile.on('open', function(fd) {


            oToFile.on('error', function(oError) {
                bError = true;
                syslog.error({action: 'KnoxedUp.getFile.write.error', message: 'failed to open file for writing: (' + sToFile + ')',
                    error: oError});
                fCallback(oError);
            });

            oToFile.on('close', function() {
                bClosed = true;
                if (!bError) {
                    syslog.debug({action: 'KnoxedUp.getFile.write.done', output: sToFile});
                    fCallback(null, sToFile);
                }
            });

            var oRequest = this._get(sFilename, sType, {}, function(oError, oResponse, sData, iRetries) {
                syslog.debug({action: 'KnoxedUp.getFile.got'});
                if (oError) {
                    syslog.error({action: 'KnoxedUp.getFile.error', error: oError});
                    bError = true;
                    oToFile.end();

                    fs.exists(sToFile, function(bExists) {
                        if (bExists) {
                            fs.unlink(sToFile, function() {
                                syslog.debug({action: 'KnoxedUp.getFile.unlink.done'});
                                fCallback(oError);
                            });
                        } else {
                            syslog.debug({action: 'KnoxedUp.getFile.done'});
                            fCallback(oError);
                        }
                    });
                } else if (!bClosed) {
                    syslog.debug({action: 'KnoxedUp.getFile.response.end'});

                    // Weird case where file may be incomplete
                    if (iRetries) {
                        syslog.debug({action: 'KnoxedUp.getFile.response.end.retried'});
                        bError = true;
                        oToFile.end();

                        fs.writeFile(sToFile, sData, sType, function(oWriteError) {
                            if (oWriteError) {
                                syslog.error({action: 'KnoxedUp.getFile.writeFile.error', error: oWriteError});
                                fCallback(oWriteError);
                            } else {
                                syslog.timeStop(iStart, {action: 'KnoxedUp.getFile.writeFile.done', output: sToFile});
                                fCallback(null, sToFile);
                            }
                        })
                    }
                }
            });

            oRequest.on('response', function(oResponse) {
                syslog.debug({action: 'KnoxedUp.getFile.response'});

                oResponse.on('data', function(sChunk) {
                    if (!bError) {
                        oToFile.write(sChunk, sType);
                    }
                });

                oResponse.on('end', function() {
                    syslog.debug({action: 'KnoxedUp.getFile.response.end'});
                    oToFile.end();
                });
            });
        }.bind(this));
    };

    /*
        Keeping putFile for backwards compatibility with knox
    */
    KnoxedUp.prototype.putFile = function (sFilename, sTo, oHeaders, fCallback) {
        var emitter = new Emitter;

        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders = {};
        }

        this._setSizeAndHashHeaders(sFilename, oHeaders, function(oError, oPreppedHeaders) {
            if (oError) {
                fCallback(oError);
            } else {
                var req = this.putStream(sFilename, sTo, oPreppedHeaders, fCallback);

                req.on('progress', emitter.emit.bind(emitter, 'progress'));
                req.on('response', emitter.emit.bind(emitter, 'response'));
                req.on('error', emitter.emit.bind(emitter, 'error'));
            }
        }.bind(this));

        return emitter;
    };

    KnoxedUp.prototype._setSizeAndHashHeaders = function (sFile, oHeaders, fCallback) {
        syslog.debug({action: 'KnoxedUp._setSizeAndHashHeaders', file: sFile, headers: oHeaders});
        async.parallel({
            stat: function (fAsyncCallback) { fs.stat(sFile, fAsyncCallback); },
            contentType: function (fAsyncCallback) {
                var contentType = mime.lookup(sFile);

                // Add charset if it's known.
                var charset = mime.charsets.lookup(contentType);
                if (charset) {
                  contentType += '; charset=' + charset;
                }

                fAsyncCallback(null, contentType);
            }
        }, function(oError, oResults) {
            if (oError) {
                syslog.error({action: 'KnoxedUp._setSizeAndHashHeaders.error', file: sFile, headers: oHeaders, error: oError});
                fCallback(oError);
            } else {
                oHeaders['Content-Length']  = oResults.stat.size;
                oHeaders['Content-Type'] = oResults.contentType;

                syslog.debug({action: 'KnoxedUp._setSizeAndHashHeaders.done', file: sFile, headers: oHeaders});
                fCallback(null, oHeaders);
            }
        });
    };

    KnoxedUp.prototype.setBucket = function(sBucket) {
        this.oConfig.bucket = sBucket;
        this.Client  = Knox.createClient(this.oConfig);
    };

    KnoxedUp.prototype.revertBucket = function() {
        this.setBucket(this.sOriginalBucket);
    };

    module.exports = KnoxedUp;

    /**
     *
     * @param {String}   sPrefix   Path of folder to list
     * @param {Integer}  iMax      Maximum number of files to show
     * @param {Function} fCallback Array of Objects in that folder
     */
    KnoxedUp.prototype.getFileList = function(sPrefix, iMax, fCallback) {
        fCallback  = typeof fCallback == 'function' ? fCallback  : function() {};
        fError     = typeof fError    == 'function' ? fError     : function() {};

        var parser    = new xml2js.Parser();

        if (KnoxedUp.isLocal()) {
            var oMatch     = new RegExp('^' + sPrefix);
            var sPathLocal = this.getLocalPath();
            var getFiles   = function(sPath, aReturn) {
                aReturn = aReturn !== undefined ? aReturn : [];
                var aFiles = fs.readdirSync(path.join(sPathLocal, sPath));
                for (var i in aFiles) {
                    var sFile      = aFiles[i];
                    var sFullFile  = path.join(sPath, sFile);
                    var sFullLocal = path.join(sPathLocal, sFullFile);
                    var oStat = fs.statSync(sFullLocal);
                    if (oStat.isDirectory()) {
                        getFiles(sFullFile, aReturn);
                    } else if (sFullFile.match(oMatch)) {
                        aReturn.push(sFullFile);
                    }
                }

                return aReturn;
            };

            fCallback(null, getFiles());
        } else {
            syslog.pause(true);
            this._get('/?prefix=' + sPrefix + '&max-keys=' + iMax, 'utf-8', {}, function(oError, oResponse, sData) {
                syslog.pause(false);
                if (oError) {
                    syslog.error({action: 'KnoxedUp.getFileList.error', error:oError});
                    fCallback(oError);
                } else {
                    parser.parseString(sData, function (oError, oResult) {
                        if (oError) {
                            fCallback(oError);
                        } else {
                            var aFiles = [];

                            if (oResult.ListBucketResult !== undefined) {
                                oResult = oResult.ListBucketResult;
                            }

                            var sKey;

                            if (oResult.Contents !== undefined) {
                                if (Array.isArray(oResult.Contents)) {
                                    for (var i in oResult.Contents) {
                                        if (oResult.Contents[i].Key) {
                                            sKey = oResult.Contents[i].Key;
                                            if (Array.isArray(sKey)) {
                                                sKey = sKey[0];
                                            }

                                            if (sKey.substr(-1) == '/') {
                                                continue;
                                            }

                                            aFiles.push(sKey)
                                        }
                                    }
                                } else {
                                    if (oResult.Contents.Key) {
                                        sKey = oResult.Contents.Key;
                                        if (Array.isArray(sKey)) {
                                            sKey = sKey[0];
                                        }

                                        if (sKey.substr(-1) != '/') {
                                            aFiles.push(sKey)
                                        }
                                    }
                                }
                            }

                            fCallback(null, aFiles);
                        }
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String} sFile       Path to file
     * @param {Function} fCallback boolean
     */
    KnoxedUp.prototype._localFileExists = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        fs.exists(this.getLocalPath(sFile), fCallback);
    };

    /**
     *
     * @param {String} sFile       Path to file
     * @param {Function} fCallback boolean
     */
    KnoxedUp.prototype.fileExists = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            this._localFileExists(sFile, fCallback);
        } else {
            this._head(sFile, {}, function(oError, oResponse) {
                if (oError) {
                    fCallback(oError);
                } else {
                    fCallback(null, oResponse.statusCode != 404);
                }
            }.bind(this));
        }
    };

    /**
     * Override me baby
     * @param {Object} oProgress
     */
    // KnoxedUp.prototype.onProgress = function(oProgress) {
    //     console.log("oProgress:", oProgress);
    // };

    /**
     *
     * @param {String}   sFrom
     * @param {String}   sTo
     * @param {Object}   oHeaders
     * @param {Function} fCallback Full contents of File
     * @param {Integer} [iRetries]
     */
    KnoxedUp.prototype.putStream = function(sFrom, sTo, oHeaders, fCallback, iRetries) {
        var bHasCallback = typeof fCallback == 'function';
        var iRetries    = iRetries !== undefined ? iRetries : 0;
        var fCallback   = bHasCallback ? fCallback : function() {};

        var oLog = {
            action:    'KnoxedUp.putStream',
            from:     sFrom,
            to:       sTo,
            headers:  oHeaders,
            retries:  iRetries,
            callback: bHasCallback
        };

        var iStart = syslog.timeStart(oLog);
        var fDone  = function(fFinishedCallback, oError, sTo) {
            if (oError) {
                syslog.error(oLog);
            } else {
                syslog.timeStop(iStart, oLog);
            }

            fFinishedCallback(oError, sTo);
        };

        if (KnoxedUp.isLocal()) {
            var sToLocal = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function(oError) {
                if (oError) {
                    syslog.error({action: 'KnoxedUp.putStream.Local.error', from: sFrom, local: sToLocal, error: oError});
                    fCallback(oError);
                } else {
                    fsX.copyFile(sFrom, sToLocal, function(oCopyError) {
                        if (oCopyError) {
                            syslog.error({action: 'KnoxedUp.putStream.Local.copy.error', from: sFrom, local: sToLocal, error: oCopyError});
                            fCallback(oError);
                        } else {
                            fCallback(null, sTo);
                        }
                    }.bind(this));
                }
            }.bind(this));
        } else {
            var oStream  = fs.createReadStream(sFrom);

            oStream.on('error', function(oError) {
                oStream.destroy();

                oLog.error = new Error(oError);
                fDone(fCallback, oLog.error);
            });

            var oRequest = this.Client.putStream(oStream, sTo, oHeaders, function(oError, oResponse) {
                oStream.destroy();

                if (oError) {
                    if (iRetries > 3) {
                        oLog.action += '.request.hang_up.retry.max';
                        oLog.error   = oError;
                        fDone(fCallback, oLog.error);
                    } else {
                        oLog.action += '.request.hang_up.retry';
                        oLog.error   = (util.isError(oError)) ? new Error(oError.message) : oError;
                        syslog.warn(oLog);
                        this.putStream(sFrom, sTo, oHeaders, fCallback, fReqCallback, iRetries + 1);
                    }
                } else if(oResponse.statusCode == 500) {
                    oLog.error   = new Error('S3 Error Code ' + oResponse.statusCode);
                    oLog.action += '.request.500.retry';
                    if (iRetries > 3) {
                        oLog.action += '.max';
                        fDone(fCallback, oLog.error);
                    } else {
                        syslog.warn(oLog);
                        this.putStream(sFrom, sTo, oHeaders, fCallback, fReqCallback, iRetries + 1);
                    }
                } else if(oResponse.statusCode > 399) {
                    oLog.error = new Error('S3 Error Code ' + oResponse.statusCode);
                    fDone(fCallback, oLog.error);
                } else {
                    oLog.action += '.done';
                    fDone(fCallback, null, sTo);
                }
            }.bind(this));

            // oRequest.on('progress', this.onProgress.bind(this));
            return oRequest;
        }
    };

    /**
     *
     * @param {String}   sFile
     * @param {Object}   oHeaders
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.updateHeaders = function(sFile, oHeaders, fCallback) {
        this.copyFile(sFile, sFile, oHeaders, fCallback);
    };

    /**
     *
     * @param {String}   sFile
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.deleteFile = function(sFile, fCallback) {
        this._delete(sFile, {}, fCallback);
    };

    /**
     *
     * @param {String} sFile
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.getHeaders = function(sFile, fCallback) {
        this._head(sFile, {}, function(oError, oResponse) {
            if (oError) {
                syslog.error({action: 'KnoxedUp.getHeaders.error', error: oError});
                fCallback(oError);
            } else {
                syslog.debug({action: 'KnoxedUp.getHeaders.done', headers: oResponse.headers});
                fCallback(null, oResponse.headers);
            }
        }.bind(this));
    };

    /**
     *
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sTo       Destination Path of File
     * @param {Object}   oHeaders
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.copyFile = function(sFrom, sTo, oHeaders, fCallback) {
        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

        fCallback = typeof fCallback == 'function' ? fCallback : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFrom)) {
            var sFromLocal = this.getLocalPath(sFrom);
            var sToLocal   = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.copyFile(sFromLocal, sToLocal, fCallback);
            }.bind(this));
        } else {
            var bHasHeaders = false;
            for (var i in oHeaders) {
                bHasHeaders = true;
                break;
            }

            oHeaders['Content-Length']           = '0';
            oHeaders['x-amz-copy-source']        = '/' + this.Client.bucket + '/' + sFrom;
            oHeaders['x-amz-metadata-directive'] = 'REPLACE';

            /*
                 No way to just over-write headers.  It's all or nothing.  This way we keep what's there and add more
                 http://doc.s3.amazonaws.com/proposals/copy.html
                 COPY: Copy the metadata from the original object. If this is specified, any metadata in this request will be ignored. This is the default.
                 REPLACE: Ignore the original object’s metadata and replace it with the metadata in this request.
             */

            this.getHeaders(sFrom, function(oGetHeadersError, oGotHeaders) {
                if (oGetHeadersError) {
                    syslog.error({action: 'KnoxedUp.copyFile.getHeaders.error', error:  oGetHeadersError});
                    fCallback(oGetHeadersError);
                } else {
                    // Copy meta-headers from original
                    for (var sKey in oGotHeaders) {
                        // Do not override headers that were explictly set by method call
                        if (oHeaders[sKey] === undefined) {
                            // These are basically just copied from the S3 console
                            if (sKey.indexOf('x-amz-meta-')         == 0
                            ||  sKey.indexOf('Content-Type')        == 0
                            ||  sKey.indexOf('Content-Disposition') == 0
                            ||  sKey.indexOf('Content-Encoding')    == 0
                            ||  sKey.indexOf('Cache-Control')       == 0
                            ||  sKey.indexOf('Expires')             == 0 ) {
                                oHeaders[sKey] = oGotHeaders[sKey];
                                bHasHeaders    = true;
                            }
                        }
                    }

                    if (!bHasHeaders) {
                        oHeaders['x-amz-metadata-directive'] = 'COPY';
                    }

                    this._put(sTo, 'utf-8', oHeaders, function(oError, oResponse, sData) {
                        if (oError) {
                            syslog.error({action: 'KnoxedUp.copyFile.error', error:  oError});
                            fCallback(oError);
                        } else {
                            fCallback(null, sData);
                        }
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sBucket   Destination Bucket
     * @param {String}   sTo       Destination Path of File
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.copyFileToBucket = function(sFrom, sBucket, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var sLocalPath = path.join(KnoxedUp.sPath, this.oConfig.bucket, sFrom);
        if (KnoxedUp.isLocal() && this._localFileExists(sLocalPath)) {
            var sFromLocal = sLocalPath;
            var sToLocal   = path.join(KnoxedUp.sPath, sBucket,             sTo);

            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.copyFile(sFromLocal, sToLocal, fCallback);
            }.bind(this));
        } else {
            var oOptions = {
                'Content-Length': '0',
                'x-amz-copy-source': '/' + this.Client.bucket + '/' + sFrom,
                'x-amz-metadata-directive': 'COPY'
            };

            var oDestination = new KnoxedUp({
                key:    this.Client.key,
                secret: this.Client.secret,
                bucket: sBucket,
                port:   80
            });

            oDestination._put(sTo, 'utf-8', oOptions, function(oError, oRequest, sData) {
                if (oError) {
                    syslog.error({action: 'KnoxedUp.copyFileToBucket.error', error:  oError});
                    fCallback(oError);
                } else {
                    syslog.debug({action: 'KnoxedUp.copyFileToBucket.done'});
                    fCallback(null, sData);
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sBucket   Destination Bucket
     * @param {String}   sTo       Destination Path of File
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.moveFileToBucket = function(sFrom, sBucket, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFrom)) {
            var sFromLocal = this.getLocalPath(sFrom);
            var sToLocal   = path.join(KnoxedUp.sPath, sBucket, sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.moveFile(sFromLocal, sToLocal, fCallback);
            }.bind(this));
        } else {
            this.copyFileToBucket(sFrom, sBucket, sTo, function(oError, sData) {
                if (oError) {
                    syslog.error({action: 'KnoxedUp.moveFileToBucket.copy.error', error: oError});
                    fCallback(oError);
                } else {
                    this._delete(sFrom, {}, function(oError) {
                        // Carry on even if delete didnt work - Error will be in the logs
                        fCallback(null, sData);
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sTo       Destination Path of File
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.moveFile = function(sFrom, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFrom)) {
            var sFromLocal = this.getLocalPath(sFrom);
            var sToLocal   = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.moveFile(sFromLocal, sToLocal, fCallback);
            }.bind(this));
        } else {
            if (sFrom == sTo) {
                fCallback(null);
            } else {
                this.copyFile(sFrom, sTo, {}, function(oError, sData) {
                    if (oError) {
                        syslog.error({action: 'KnoxedUp.moveFile', from: sFrom, to: sTo, error: oError});
                        fCallback(oError);
                    } else {
                        this._delete(sFrom, {}, function(oError) {
                            // Carry on even if delete didnt work - Error will be in the logs
                            fCallback(null, sData);
                        }.bind(this));
                    }
                }.bind(this));
            }
        }
    };

    KnoxedUp.prototype._checkHash = function(sHash, sCheckHash, fCallback) {
        if (sCheckHash !== null) {
            if (sHash !== sCheckHash) {
                var oError = new Error('File Hash Mismatch');
                syslog.error({
                    action: 'KnoxedUp._checkHash.error',
                    hash: {
                        check:  sCheckHash,
                        actual: sHash
                    },
                    error: oError
                });
                return fCallback(oError);
            }
        }

        fCallback(null, sHash);
    };

    /**
     *
     * @param {String}   sFile     Path to File to Download
     * @param {String}   sType     Binary or (?)
     * @param {String}   sCheckHash
     * @param {String|Function}   [sExtension]
     * @param {Function} fCallback - Path of Temp File
     */
    KnoxedUp.prototype.toTemp = function(sFile, sType, sCheckHash, sExtension, fCallback) {
        if (typeof sExtension == 'function') {
            fCallback         = sExtension;
            sExtension        = path.extname(sFile);
        }

        fCallback  = typeof fCallback       == 'function' ? fCallback        : function() {};
        sExtension = KnoxedUp._dotExtension(sExtension);

        var sTempFile  = fsX.getTmpSync() + sFile.split('/').pop();

        syslog.debug({action: 'KnoxedUp.toTemp', file: sFile, type: sType, extension: sExtension, temp: sTempFile});

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            this._fromTemp(this.getLocalPath(sFile), sCheckHash, sExtension, fCallback);
        } else {
            this._getCachedFile(sCheckHash, sExtension, function(oCachedError, sCachedFile) {
                if (sCachedFile) {
                    fCallback(null, sCachedFile, sCheckHash);
                } else {
                    this._toTemp(sTempFile, sFile, sType, sCheckHash, sExtension, fCallback);
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String} sTempFile
     * @param {String} sCheckHash
     * @param {String} sExtension
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._fromTemp = function(sTempFile, sCheckHash, sExtension, fCallback) {
        syslog.debug({action: 'KnoxedUp._fromTemp', file: sTempFile});
        var iStart = syslog.timeStart();

        sExtension = KnoxedUp._dotExtension(sExtension);

        async.auto({
            hash:           function(fAsyncCallback, oResults) { fsX.hashFile(sTempFile, fAsyncCallback) },
            check: ['hash', function(fAsyncCallback, oResults) { this._checkHash (oResults.hash, sCheckHash, fAsyncCallback) }.bind(this)],
            copy:  ['hash', function(fAsyncCallback, oResults) { fsX.copyFile(sTempFile,  fsX.getTmpSync() + oResults.hash + sExtension, fAsyncCallback) }],
            chmod: ['copy', function(fAsyncCallback, oResults) { fs.chmod(oResults.copy, 0777, fAsyncCallback) }]
        }, function(oError, oResults) {
            if (oError) {
                syslog.error({action: 'KnoxedUp._fromTemp.error', error: oError});
                fCallback(oError);
            } else {
                syslog.timeStop(iStart, {action: 'KnoxedUp._fromTemp.done', hash: oResults.hash, file: oResults.copy});
                fCallback(null, oResults.copy, oResults.hash);
            }
        });
    };

    KnoxedUp._dotExtension = function(sExtension) {
        if (sExtension
        &&  sExtension.length) {
            return '.' + sExtension.replace(/^\.+/g, '');
        }

        return sExtension;
    };

    KnoxedUp._getTmpCache = function() {
        return '/tmp/cameo/cache/';
    };

    /**
     *
     * @param {String} sHash
     * @param {String} sExtension
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._getCachedFile = function(sHash, sExtension, fCallback) {
        return fCallback(null, null);

        syslog.debug({action: 'KnoxedUp._getCachedFile', hash: sHash, extension: sExtension});

        sExtension = KnoxedUp._dotExtension(sExtension);

        var sCachedFile  = path.join(KnoxedUp._getTmpCache(), sHash + sExtension);
        var sDestination = path.join(fsX.getTmpSync(), sHash + sExtension);
        fs.exists(sCachedFile, function(bExists) {
            if (bExists) {
                fsX.hashFile(sCachedFile, function(oHashError, sFileHash) {
                    if (oHashError) {
                        fCallback(oHashError);
                    } else if (sHash == sFileHash) {
                        syslog.debug({action: 'KnoxedUp._getCachedFile.found', file: sDestination});
                        fCallback(null, sCachedFile, sFileHash);
                    } else {
                        fCallback(null, null);
                    }
                }.bind(this));
            } else {
                fCallback(null, null);
            }
        }.bind(this));
    };

    /**
     *
     * @param {String} sFile
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._cacheFile = function(sFile, fCallback) {
        return fCallback(null);

        syslog.debug({action: 'KnoxedUp._cacheFile', file: sFile});
        var iStart = syslog.timeStart();

        fsX.mkdirP(KnoxedUp._getTmpCache(), 0777, function(oError, sPath) {
            if (oError) {
                fCallback(oError);
            } else {
                var sDestination = path.join(KnoxedUp._getTmpCache(), path.basename(sFile));
                fsX.copyFile(sFile, sDestination, function(oCopyError, oResult) {
                    if (oCopyError) {
                        fCallback(oCopyError);
                    } else {
                        syslog.timeStop(iStart, {action: 'KnoxedUp._cacheFile.cached', file: sFile});
                        fCallback(null);
                    }
                });
            }
        });
    };

    /**
     *
     * @param {String} sTempFile
     * @param {String} sFile
     * @param {String} sType
     * @param {String} sCheckHash
     * @param {String} sExtension
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._toTemp = function(sTempFile, sFile, sType, sCheckHash, sExtension, fCallback) {
        syslog.debug({action: 'KnoxedUp._toTemp', file: sTempFile, s3: sFile, type: sType});
        var iStart = syslog.timeStart();

        sExtension = KnoxedUp._dotExtension(sExtension);

        async.auto({
            get:             function(fAsyncCallback, oResults) { this.getFile(sFile, sTempFile, sType, fAsyncCallback) }.bind(this),
            move:  ['get',   function(fAsyncCallback, oResults) { fsX.moveFileToHash(oResults.get, fsX.getTmpSync(), sExtension, fAsyncCallback) }],
            check: ['move',  function(fAsyncCallback, oResults) { this._checkHash (oResults.move.hash, sCheckHash, fAsyncCallback) }.bind(this)],
            cache: ['check', function(fAsyncCallback, oResults) { this._cacheFile(oResults.move.path, fAsyncCallback) }.bind(this)]
        }, function(oError, oResults) {
            if (oError) {
                syslog.error({action: 'KnoxedUp._toTemp.error', error: oError, hash: sCheckHash, results: oResults});
                fCallback(oError);
            } else {
                syslog.timeStop(iStart, {action: 'KnoxedUp._toTemp.done', hash: oResults.hash, file: oResults.copy});
                fCallback(null, oResults.move.path, oResults.move.hash);
            }
        });
    };

    /**
     *
     * @param {String}   sFile     Path to File to Download
     * @param {String}   sType     Binary or (?)
     * @param {Function} fCallback - Path of Temp File
     */
    KnoxedUp.prototype.toSha1 = function(sFile, sType, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback : function() {};
        sType     = sType || 'binary';

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            fsX.hashFile(this.getLocalPath(sFile), fCallback);
        } else {
            this.toTemp(sFile, sType, null, function(oError, sTempFile, sHash) {
                if (oError) {
                    fCallback(oError);
                } else {
                    fCallback(null, sHash);
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {Array}    aFiles    Array if file paths to download to temp
     * @param {String}   sType     Binary or (?)
     * @param {Function} fCallback Object of Temp Files with S3 file names as Key
     */
    KnoxedUp.prototype.filesToSha1 = function(aFiles, sType, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var oTempFiles = {};
        async.forEach(aFiles, function (sFile, fCallbackAsync) {
            this.toSha1(sFile, sType, function(oError, sHash) {
                if (oError) {
                    fCallbackAsync(oError);
                } else {
                    oTempFiles[sFile] = sHash;
                    fCallbackAsync(null);
                }
            }.bind(this))
        }.bind(this), function(oError) {
            if (oError) {
                fCallback(oError);
            } else {
                fCallback(null, oTempFiles);
            }
        }.bind(this));
    };

    /**
     *
     * @param {Object}   oFiles    Object of file paths to download to temp with file hashes as the key
     * @param {String}   sType     Binary or (?)
     * @param {Function} fCallback Object of Temp Files with S3 file names as Key
     */
    KnoxedUp.prototype.filesToTemp = function(oFiles, sType, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var aDownloads = [];
        for (var sHash in oFiles) {
            aDownloads.push({
                hash: sHash,
                file: oFiles[sHash]
            });
        }

        var oTempFiles = {};
        async.forEach(aDownloads, function (oFile, fCallbackAsync) {
            this.toTemp(oFile.file, sType, oFile.hash, function(oError, sTempFile) {
                if (oError) {
                    fCallbackAsync(oError);
                } else {
                    oTempFiles[oFile.hash] = sTempFile;
                    fCallbackAsync(null);
                }
            }.bind(this))
        }.bind(this), function(oError) {
            if (oError) {
                syslog.error({action: 'KnoxedUp.filesToTemp.error', error: oError});
            } else {
                fCallback(oError, oTempFiles);
            }
        }.bind(this));
    };

    /**
     *
     * @param {Object}   oFiles    Object of file paths to download to temp with file hashes as the key
     * @param {String}   sType     Binary or (?)
     * @param {String}   sExtension
     * @param {Function} fCallback Object of Temp Files with S3 file names as Key
     */
    KnoxedUp.prototype.filesToTempWithExtension = function(oFiles, sType, sExtension, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var aDownloads = [];
        for (var sHash in oFiles) {
            aDownloads.push({
                hash: sHash,
                file: oFiles[sHash]
            });
        }

        var oTempFiles = {};
        async.forEach(aDownloads, function (oFile, fCallbackAsync) {
            this.toTemp(oFile.file, sType, oFile.hash, sExtension, function(oError, sTempFile) {
                if (oError) {
                    fCallbackAsync(oError);
                } else {
                    oTempFiles[oFile.hash] = sTempFile;
                    fCallbackAsync(null);
                }
            }.bind(this))
        }.bind(this), function(oError) {
            if (oError) {
                syslog.error({action: 'KnoxedUp.filesToTempWithExtension.error', error: oError});
            }

            fCallback(oError, oTempFiles);
        }.bind(this));
    };

    KnoxedUp.prototype.getLocalPath = function(sFile) {
        sFile = sFile !== undefined ? sFile : '';

        return path.join(KnoxedUp.sPath, this.oConfig.bucket, sFile);
    };

    KnoxedUp.isLocal = function() {
        return KnoxedUp.bLocal
            && KnoxedUp.sPath.length > 0;
    };

    KnoxedUp.setLocal = function(bLocal, sPath) {
        KnoxedUp.bLocal = bLocal;
        KnoxedUp.sPath  = sPath;
    };

    KnoxedUp.bLocal = false;
    KnoxedUp.sPath  = '';
