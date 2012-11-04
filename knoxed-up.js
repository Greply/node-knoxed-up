    var fs          = require('fs');
    var path        = require('path');
    var Knox        = require('knox');
    var fsX         = require('fs-extended');
    var syslog      = require('syslog-console').init('KnoxedUp');
    var xml2js      = require('xml2js');
    var async       = require('async');
    var Buffer      = require('buffer').Buffer;

    var KnoxedUp = function(config) {
        this.oConfig = config;
        this.sOriginalBucket = this.oConfig.bucket;
        this.Client  = Knox.createClient(this.oConfig);

        if (config.local !== undefined
        &&  config.path  !== undefined) {
            KnoxedUp.setLocal(config.local, config.path);
        }
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
     * @param {Integer}  iRetries
     */
    KnoxedUp.prototype.getFileList = function(sPrefix, iMax, fCallback, iRetries) {
        iRetries   = iRetries !== undefined ? iRetries : 3;
        fCallback  = typeof fCallback == 'function' ? fCallback  : function() {};
        fError     = typeof fError    == 'function' ? fError     : function() {};

        var parser    = new xml2js.Parser();
        var sContents = '';

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
            var oRequest = this.get('/?prefix=' + sPrefix + '&max-keys=' + iMax);

            oRequest.on('error', function(oError) {
                if (oError.message == 'socket hang up') {
                    if (iRetries > 0) {
                        syslog.warn({action: 'KnoxedUp.getFileList.request.error.hang_up.retry', retries: iRetries});
                        this.getFileList(sPrefix, iMax, fCallback, iRetries - 1);
                    } else {
                        syslog.error({action: 'KnoxedUp.getFileList.request.error.hang_up.retry.max', error: oError});
                        fCallback(oError);
                    }
                } else {
                    syslog.error({action: 'KnoxedUp.getFileList.request.error', error: oError});
                    fCallback(oError);
                }
            }.bind(this));

            oRequest.on('response', function(oResponse) {
                syslog.debug({action: 'KnoxedUp.getFileList.downloading',  status: oResponse.statusCode});

                if(oResponse.statusCode > 399) {
                    var oError = new Error('S3 Error Code ' + oResponse.statusCode);
                    syslog.error({action: 'KnoxedUp.getFileList.download.error', error: oError});
                    fCallback(oError);
                } else {
                    oResponse.setEncoding('utf8');
                    oResponse
                        .on('data', function(sChunk){
                            sContents += sChunk;
                        })
                        .on('error', function(oError){
                            syslog.error({action: 'KnoxedUp.getFileList.download.error', error:oError});
                            fCallback(oError);
                        })
                        .on('end', function() {
                            parser.parseString(sContents, function (oError, oResult) {
                                if (oError) {
                                    fError(oError);
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
                            });
                        });
                }
            }).end();
        }
    };

    KnoxedUp.prototype.get = function (sFile) {
        if (!sFile.match(/^\//)) {
            sFile = '/' + sFile;
        }

        return this.Client.get(sFile);
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
            var oRequest = this.Client.head(sFile);

            oRequest.on('error', function(oError) {
                syslog.error({action: 'KnoxedUp.fileExists.error', error: oError});
            });

            oRequest.on('response', function(oResponse) {
                fCallback(oResponse.statusCode != 404);
            }).end();
        }
    };

    /**
     *
     * @param {String}   sFrom
     * @param {String}   sTo
     * @param {Object}   oHeaders
     * @param {Function} fCallback Full contents of File
     */
    KnoxedUp.prototype.putStream = function(sFrom, sTo, oHeaders, fCallback) {
        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

        fCallback = typeof fCallback == 'function' ? fCallback : function() {};

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
                    });
                }
            });
        } else {
            var oStream = fs.createReadStream(sFrom);
            this.Client.putStream(oStream, sTo, oHeaders, function(oError) {
                oStream.destroy();
                if (oError) {
                    syslog.error({action: 'KnoxedUp.putStream.error', from: sFrom, local: sToLocal, error: oError});
                    fCallback(oError);
                } else {
                    fCallback(null, sTo);
                }
            });
        }
    };

    /**
     *
     * @param {String}   sFile     Path to File
     * @param {Function} fCallback Full contents of File
     * @param {Number} iRetries
     */
    KnoxedUp.prototype.getFile = function(sFile, fCallback, iRetries) {
        iRetries  = iRetries !== undefined ? iRetries : 3;
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            fCallback(fs.readFileSync(this.getLocalPath(sFile)));
        } else {
            var oRequest = this.get(sFile);

            oRequest.on('error', function(oError) {
                if (oError.message == 'socket hang up') {
                    if (iRetries > 0) {
                        syslog.warn({action: 'KnoxedUp.getFile.request.hang_up.retry', file: sFile, retries: iRetries});
                        this.getFile(sFile, fCallback, iRetries - 1);
                    } else {
                        syslog.error({action: 'KnoxedUp.getFile.request.hang_up.retry.max', error: oError});
                        fCallback(oError);
                    }
                } else {
                    syslog.error({action: 'KnoxedUp.getFile.request.error', error: oError, file: sFile});
                    fCallback(oError);
                }
            }.bind(this));

            oRequest.on('response', function(oResponse) {
                if(oResponse.statusCode > 399) {
                    syslog.error({action: 'KnoxedUp.getFile.error', status: oResponse.statusCode});

                    switch(oResponse.statusCode) {
                        case 404:
                            fCallback(new Error('File Not Found'));
                            break;

                        default:
                            fCallback(new Error('S3 Error Code ' + oResponse.statusCode));
                            break;
                    }
                } else {
                    var sContents = '';
                    oResponse.setEncoding('utf8');
                    oResponse
                        .on('data', function(sChunk){
                            sContents += sChunk;
                        })
                       .on('error', function(oError){
                            syslog.error({action: 'KnoxedUp.getFile.error', error: oError});
                            fCallback(oError);
                        })
                        .on('end', function(sChunk){
                            fCallback(null, sContents);
                        });
                }
            }).end();
        }
    };

    /**
     *
     * @param {Array}    aFiles    - Array of filenames to retrieve
     * @param {Function} fCallback - Contents object with filename as key and file contents as value
     */
    KnoxedUp.prototype.getFiles = function(aFiles, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var oContents = {};
        var iFiles    = aFiles.length;
        if (iFiles) {
            async.forEach(aFiles, function(sFile, fGetCallback) {
                this.getFile(sFile, function(oError, sContents) {
                    if (oError) {
                        fGetCallback(oError);
                    } else {
                        oContents[sFile] = sContents;
                        fGetCallback(null);
                    }
                });
            }.bind(this), function(oError) {
                if (oError) {
                    fCallback(oError);
                } else {
                    fCallback(null, oContents);
                }
            }.bind(this));
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
     * @param {String} sFile
     * @param {Function} fCallback
     * @param {Integer} iRetries
     */
    KnoxedUp.prototype.getHeaders = function(sFile, fCallback, iRetries) {
        iRetries  = iRetries !== undefined ? iRetries : 3;
        var oRequest = this.Client.head('/' + sFile);

        oRequest.on('error', function(oError) {
            if (oError.message == 'socket hang up') {
                if (iRetries > 0) {
                    syslog.warn({action: 'KnoxedUp.getHeaders.request.hang_up.retry', file: sFile, retries: iRetries});
                    this.getHeaders(sFile, fCallback, iRetries - 1);
                } else {
                    syslog.error({action: 'KnoxedUp.getHeaders.request.hang_up.retry.max', error: oError});
                    fCallback(oError);
                }
            } else {
                syslog.error({action: 'KnoxedUp.getHeaders.request.error', error: oError, file: sFile});
                fCallback(oError);
            }
        }.bind(this));

        oRequest.on('response', function(oResponse) {
            syslog.debug({action: 'KnoxedUp.getHeaders', status: oResponse.statusCode});

            if(oResponse.statusCode > 399) {
                syslog.error({action: 'KnoxedUp.getHeaders.error', status: oResponse.statusCode});

                switch(oResponse.statusCode) {
                    case 404:
                        fCallback(new Error('File Not Found'));
                        break;

                    default:
                        fCallback(new Error('S3 Error Code ' + oResponse.statusCode));
                        break;
                }
            } else {
                oResponse
                    .on('error', function(oError){
                        syslog.error({action: 'KnoxedUp.getHeaders.error', error: oError});
                        fCallback(oError);
                    })
                    .on('end', function(){
                        syslog.debug({action: 'KnoxedUp.getHeaders.done', headers: oResponse.headers});
                        fCallback(null, oResponse.headers);
                    });
            }
        }).end();

        return oRequest;
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
            });
        } else {
            var bHasHeaders = false;
            for (var i in oHeaders) {
                bHasHeaders = true;
                break;
            }

            oHeaders['Content-Length']           = '0';
            oHeaders['x-amz-copy-source']        = '/' + this.Client.bucket + '/' + sFrom;
            oHeaders['x-amz-metadata-directive'] = bHasHeaders ? 'REPLACE' : 'COPY';

            this.Client.put(sTo, oHeaders).on('response', function(oResponse) {
                oResponse.setEncoding('utf8');
                oResponse.on('error', function(oError){
                    syslog.error({action: 'KnoxedUp.copyFile.error', error:  oError});
                    fCallback(oError);
                });
                oResponse.on('data', function(oChunk){
                    fCallback(null, oChunk);
                });
            }).end();
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
            });
        } else {
            var oOptions = {
                'Content-Length': '0',
                'x-amz-copy-source': '/' + this.Client.bucket + '/' + sFrom,
                'x-amz-metadata-directive': 'COPY'
            };

            var oDestination = Knox.createClient({
                key:    this.Client.key,
                secret: this.Client.secret,
                bucket: sBucket
            });

            var oRequest = oDestination.put(sTo, oOptions);

            oRequest.on('error', function(oError) {
                if (oError.message == 'socket hang up') {
                    syslog.error({action: 'KnoxedUp.getHeaders.request.hang_up.retry.max', error: oError});
                    fCallback(oError);
                } else {
                    syslog.error({action: 'KnoxedUp.getHeaders.request.error', error: oError, file: sFile});
                    fCallback(oError);
                }
            }.bind(this));

            oRequest.on('response', function(oResponse) {
                oResponse.setEncoding('utf8');
                oResponse.on('data', function(oChunk){
                    fCallback(null, oChunk);
                });
                oResponse.on('error', function(oError){
                    syslog.error({action: 'KnoxedUp.copyFileToBucket.error', error:  oError});
                    fCallback(oError);
                });
            }).end();
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
            });
        } else {
            this.copyFileToBucket(sFrom, sBucket, sTo, function(oError, oChunk) {
                if (oError) {
                    syslog.error({action: 'KnoxedUp.moveFileToBucket.copy.error', error: oError});
                    fCallback(oError);
                } else {
                    var oDeleteRequest = this.Client.del(sFrom).end();
                    oDeleteRequest.on('error', function(oError) {
                        syslog.error({action: 'KnoxedUp.moveFileToBucket.delete.error', error:  oError});
                    }.bind(this));

                    fCallback(null, oChunk);
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sTo       Destination Path of File
     * @param {Object}   oHeaders
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.moveFile = function(sFrom, sTo, oHeaders, fCallback) {
        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

        fCallback = typeof fCallback == 'function' ? fCallback : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFrom)) {
            var sFromLocal = this.getLocalPath(sFrom);
            var sToLocal   = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.moveFile(sFromLocal, sToLocal, fCallback);
            });
        } else {
            if (sFrom == sTo) {
                fCallback(null);
            } else {
                this.copyFile(sFrom, sTo, oHeaders, function(oError, oChunk) {
                    if (oError) {
                        syslog.error({action: 'KnoxedUp.moveFile', from: sFrom, to: sTo, error: oError});
                        fCallback(oError);
                    } else {
                        var oDeleteRequest = this.Client.del(sFrom).end();
                        oDeleteRequest.on('error', function(oError) {
                            syslog.error({
                                 action: 'KnoxedUp.moveFile.delete.error',
                                 error:  oError
                             });
                        }.bind(this));

                        fCallback(null, oChunk);
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

        fCallback(null);
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

        syslog.debug({action: 'KnoxedUp.toTemp', file: sFile, type: sType, extension: sExtension});
        fCallback       = typeof fCallback       == 'function' ? fCallback        : function() {};

        var sTempFile  = '/tmp/' + sFile.split('/').pop();

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            fsX.hashFile(this.getLocalPath(sFile), function(oError, sHash) {
                if (oError) {
                    syslog.error({action: 'KnoxedUp._fromTemp.hash.error', error: oError});
                    fCallback(oError);
                } else {
                    this._checkHash(sHash, sCheckHash, function(oCheckHashError) {
                        if (oCheckHashError) {
                            fCallback(oCheckHashError);
                        } else {
                            var sFinalFile = '/tmp/' + sHash + sExtension;
                            fsX.copyFile(this.getLocalPath(sFile), sFinalFile, function(oError) {
                                if (oError) {
                                    syslog.error({action: 'KnoxedUp._fromTemp.copy.error', error: oError});
                                    fCallback(oError);
                                } else {
                                    fs.chmod(sFinalFile, 0777, function(oError) {
                                        if (oError) {
                                            syslog.error({action: 'KnoxedUp._fromTemp.chmod.error', error: oError});
                                            fCallback(oError);
                                        } else {
                                            fCallback(null, sFinalFile, sHash);
                                        }
                                    }.bind(this));
                                }
                            }.bind(this));
                        }
                    }.bind(this));
                }
            }.bind(this));
        } else {
            fs.exists(sTempFile, function(bExists) {
                if (bExists) {
                    this._fromTemp(sTempFile, sCheckHash, sExtension, fCallback);
                } else {
                    fs.exists(sTempFile + sExtension, function(bExists) {
                        if (bExists) {
                            this._fromTemp(sTempFile + sExtension, sCheckHash, sExtension, fCallback);
                        } else {
                            this._toTemp(sTempFile, sFile, sType, sCheckHash, sExtension, fCallback);
                        }
                    }.bind(this));
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
        fsX.hashFile(sTempFile, function(oError, sHash) {
            if (oError) {
                syslog.error({action: 'KnoxedUp._fromTemp.hash.error', error: oError});
                fCallback(oError);
            } else {
                this._checkHash(sHash, sCheckHash, function(oCheckHashError) {
                    if (oCheckHashError) {
                        fCallback(oCheckHashError);
                    } else {
                        var sFinalFile = '/tmp/' + sHash + sExtension;
                        fsX.copyFile(sTempFile, sFinalFile, function(oError) {
                            if (oError) {
                                syslog.error({action: 'KnoxedUp._fromTemp.copy.error', error: oError});
                                fCallback(oError);
                            } else {
                                fs.chmod(sFinalFile, 0777, function(oError) {
                                    if (oError) {
                                        syslog.error({action: 'KnoxedUp._fromTemp.chmod.error', error: oError});
                                        fCallback(oError);
                                    } else {
                                        syslog.timeStop(iStart, {action: 'KnoxedUp._fromTemp.done', hash: sHash, file: sFinalFile});
                                        fCallback(null, sFinalFile, sHash);
                                    }
                                }.bind(this));
                            }
                        }.bind(this));
                    }
                }.bind(this));
            }
        }.bind(this));
    };


    /**
     *
     * @param {String} sTempFile
     * @param {String} sFile
     * @param {String} sType
     * @param {String} sCheckHash
     * @param {String} [sExtension]
     * @param {Function} fCallback
     * @param {Number} iRetries
     * @private
     */
    KnoxedUp.prototype._toTemp = function(sTempFile, sFile, sType, sCheckHash, sExtension, fCallback, iRetries) {
            iRetries          = iRetries !== undefined ? iRetries : 3;
        var iStart            = syslog.timeStart();
        var iLengthTotal      = 0;
        var iLengthDownloaded = 0;
        var bRetry            = false;
        var oStream    = fs.createWriteStream(sTempFile, {
            flags:      'w',
            encoding:   sType,
            mode:       0777
        });

        oStream.on('close', function() {
            if (bRetry) {
                syslog.debug({action: 'KnoxedUp._toTemp.download.halted'});
            } else if (iLengthDownloaded < iLengthTotal) {
                var oError = new Error('Download Length did not match Content Length');
                syslog.error({action: 'KnoxedUp._toTemp.download.error', error: oError, length: {download: iLengthDownloaded, total: iLengthTotal}});
                fsX.removeDirectory(sTempFile, function() {
                    fCallback(oError);
                });
            } else {
                syslog.debug({action: 'KnoxedUp._toTemp.downloaded', size: iLengthDownloaded, file: sTempFile});
                fsX.moveFileToHash(sTempFile, '/tmp', sExtension, function(oMoveError, oDestination) {
                    if (oMoveError) {
                        fCallback(oMoveError);
                    } else {
                        this._checkHash(oDestination.hash, sCheckHash, function(oCheckHashError) {
                            if (oCheckHashError) {
                                fCallback(oCheckHashError);
                            } else {
                                syslog.timeStop(iStart, {action: 'KnoxedUp._toTemp.done', hash: oDestination.hash, file: oDestination.path});
                                fCallback(null, oDestination.path, oDestination.hash);
                            }
                        }.bind(this));
                    }
                }.bind(this));
            }
        }.bind(this));

        syslog.debug({action: 'KnoxedUp._toTemp', file: sTempFile, s3: sFile});

        var oRequest = this.get('/' + sFile);

        oRequest.on('error', function(oError) {
            if (oError.message == 'socket hang up') {
                if (iRetries > 0) {
                    syslog.warn({action: 'KnoxedUp._toTemp.request.hang_up.retry', file: sFile, retries: iRetries});
                    bRetry = true;
                    oStream.destroy();
                    this._toTemp(sTempFile, sFile, sType, sCheckHash, sExtension, fCallback, iRetries - 1);
                } else {
                    syslog.error({action: 'KnoxedUp._toTemp.request.hang_up.retry.max', error: oError});
                    oStream.destroy();
                    fCallback(oError);
                }
            } else {
                syslog.error({action: 'KnoxedUp._toTemp.request.error', error: oError, file: sFile});
                fCallback(oError);
            }
        }.bind(this));

        oRequest.on('response', function(oResponse) {
            iLengthTotal = parseInt(oResponse.headers['content-length'], 10);
            syslog.debug({action: 'KnoxedUp._toTemp.downloading', size: iLengthTotal, status: oResponse.statusCode});

            if(oResponse.statusCode > 399) {
                syslog.error({action: 'KnoxedUp._toTemp.download.error', status: oResponse.statusCode});
                var oError;

                switch(oResponse.statusCode) {
                    case 404:  oError = new Error('File Not Found');                         break;
                    default:   oError = new Error('S3 Error Code ' + oResponse.statusCode);  break;
                }
                syslog.error({action: 'KnoxedUp._toTemp.download.error', error: oError, file: sFile});
                fCallback(oError);
            } else {
                oResponse.setEncoding(sType);
                oResponse
                    .on('data', function(sChunk){
                        iLengthDownloaded += sChunk.length;
                        oStream.write(sChunk, sType);
                    })
                    .on('error', function(oError){
                        syslog.error({action: 'KnoxedUp._toTemp.download.error', error:oError});
                        fCallback(oError);
                    })
                    .on('end', function(){
                        oStream.end();
                    });
            }
        }).end();

        return oRequest;
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
            });
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
            fCallback(oError, oTempFiles);
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
