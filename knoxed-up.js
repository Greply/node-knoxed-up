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
     * @param {Function} fError
     */
    KnoxedUp.prototype.getFileList = function(sPrefix, iMax, fCallback, fError) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};
        fError    = typeof fError    == 'function' ? fError     : function() {};

        var parser = new xml2js.Parser();

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

            fCallback(getFiles());
        } else {
            this.get('/?prefix=' + sPrefix + '&max-keys=' + iMax).on('response', function(oResponse) {
                var sContents = '';
                oResponse.setEncoding('utf8');
                oResponse
                    .on('data', function(sChunk){
                        sContents += sChunk;
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

                                fCallback(aFiles);
                            }
                        });
                    });
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
            this.Client.head(sFile).on('response', function(oResponse) {
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
        fCallback = typeof oHeaders == 'function' ? oHeaders : fCallback;

        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

        if (KnoxedUp.isLocal()) {
            var sToLocal = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function(oError) {
                if (oError) {
                    console.error('putStream.Local.error', sFrom, sToLocal, oError);
                    fCallback(oError, sTo);
                } else {
                    fsX.copyFile(sFrom, sToLocal, function() {
                        fCallback(null, sTo);
                    });
                }
            });
        } else {
            this.Client.putStream(fs.createReadStream(sFrom), sTo, oHeaders, function(oError) {
                fCallback(oError, sTo);
            });
        }
    };

    /**
     *
     * @param {String}   sFile     Path to File
     * @param {Function} fCallback Full contents of File
     */
    KnoxedUp.prototype.getFile = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            fCallback(fs.readFileSync(this.getLocalPath(sFile)));
        } else {
            this.get(sFile).on('response', function(oResponse) {
                var sContents = '';
                oResponse.setEncoding('utf8');
                oResponse
                    .on('data', function(sChunk){
                        sContents += sChunk;
                    })
                    .on('end', function(sChunk){
                        fCallback(sContents);
                    });
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
                this.getFile(sFile, function(sContents) {
                    oContents[sFile] = sContents;
                    fGetCallback(null);
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
     * @param {String}   sFrom     Path of File to Move
     * @param {String}   sTo       Destination Path of File
     * @param {Object}   oHeaders
     * @param {Function} fCallback
     */
    KnoxedUp.prototype.copyFile = function(sFrom, sTo, oHeaders, fCallback) {
        fCallback = typeof oHeaders == 'function' ? oHeaders : fCallback;

        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

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

            oDestination.put(sTo, oOptions).on('response', function(oResponse) {
                oResponse.setEncoding('utf8');
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
                this.Client.del(sFrom).end();
                fCallback(oError, oChunk);
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
        fCallback = typeof oHeaders == 'function' ? oHeaders : fCallback;

        if (typeof oHeaders == 'function') {
            fCallback = oHeaders;
            oHeaders  = {};
        }

        if (KnoxedUp.isLocal() && this._localFileExists(sFrom)) {
            var sFromLocal = this.getLocalPath(sFrom);
            var sToLocal   = this.getLocalPath(sTo);
            fsX.mkdirP(path.dirname(sToLocal), 0777, function() {
                fsX.moveFile(sFromLocal, sToLocal, fCallback);
            });
        } else {
            if (sFrom == sTo) {
                fCallback();
            } else {
                this.copyFile(sFrom, sTo, oHeaders, function(oError, oChunk) {
                    this.Client.del(sFrom).end();
                    fCallback(oError, oChunk);
                }.bind(this));
            }
        }
    };

    /**
     *
     * @param {String}   sFile     Path to File to Download
     * @param {String}   sType     Binary or (?)
     * @param {String}   sExtension
     * @param {Function} fCallback - Path of Temp File
     */
    KnoxedUp.prototype.toTemp = function(sFile, sType, sExtension, fCallback) {
        sType           = sType || 'binary';

        if (typeof sExtension == 'function') {
            fCallback         = sExtension;
            sExtension        = path.extname(sFile);
        }

        syslog.debug({action: 'KnoxedUp.toTemp', file: sFile, type: sType, extension: sExtension});
        fCallback       = typeof fCallback       == 'function' ? fCallback        : function() {};

        var sTempFile  = '/tmp/' + sFile.split('/').pop();

        if (KnoxedUp.isLocal() && this._localFileExists(sFile)) {
            fsX.hashFile(this.getLocalPath(sFile), function(oError, sHash) {
                var sFinalFile = '/tmp/' + sHash + sExtension;
                fsX.copyFile(this.getLocalPath(sFile), sFinalFile, function() {
                    fs.chmod(sFinalFile, 0777, function() {
                        fCallback(sFinalFile, sHash);
                    });
                });
            });
        } else {
            fs.exists(sTempFile, function(bExists) {
                if (bExists) {
                    this._fromTemp(sTempFile, sExtension, fCallback);
                } else {
                    fs.exists(sTempFile + sExtension, function(bExists) {
                        if (bExists) {
                            this._fromTemp(sTempFile + sExtension, sExtension, fCallback);
                        } else {
                            this._toTemp(sTempFile, sFile, sType, sExtension, fCallback);
                        }
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    /**
     *
     * @param {String} sTempFile
     * @param {String} sExtension
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._fromTemp = function(sTempFile, sExtension, fCallback) {
        syslog.debug({action: 'KnoxedUp._fromTemp', file: sTempFile});
        var iStart = syslog.timeStart();
        fsX.hashFile(sTempFile, function(oError, sHash) {
            var sFinalFile = '/tmp/' + sHash + sExtension;
            fsX.copyFile(sTempFile, sFinalFile, function() {
                fs.chmod(sFinalFile, 0777, function() {
                    syslog.timeStop(iStart, {action: 'KnoxedUp._fromTemp.done', hash: sHash, file: sFinalFile});
                    fCallback(sFinalFile, sHash);
                });
            });
        });
    };


    /**
     *
     * @param {String} sTempFile
     * @param {String} sFile
     * @param {String} sType
     * @param {String} sExtension
     * @param {Function} fCallback
     * @private
     */
    KnoxedUp.prototype._toTemp = function(sTempFile, sFile, sType, sExtension, fCallback) {
        var iStart = syslog.timeStart();
        var oStream    = fs.createWriteStream(sTempFile, {
            flags:      'w',
            encoding:   sType,
            mode:       0777
        });

        syslog.debug({action: 'KnoxedUp._toTemp', file: sTempFile, s3: sFile});

        var oRequest = this.get('/' + sFile);
        oRequest.on('response', function(oResponse) {
            syslog.debug({action: 'KnoxedUp._toTemp.downloading', size: oResponse.headers['content-length'], status: oResponse.statusCode});

            if(oResponse.statusCode == 400) {
                syslog.error({action: 'KnoxedUp._toTemp.download.error', status: oResponse.statusCode});
                fCallback();
            } else {
                oResponse.setEncoding(sType);
                oResponse
                    .on('data', function(sChunk){
                        oStream.write(sChunk, sType);
                    })
                    .on('error', function(oError){
                        syslog.error({action: 'KnoxedUp._toTemp.download.error', error:oError});
                        fCallback();
                    })
                    .on('end', function(){
                        oStream.end();
                        syslog.debug({action: 'KnoxedUp._toTemp.downloaded', file: sTempFile});
                        fsX.hashFile(sTempFile, function(oError, sHash) {
                            var sFinalFile = '/tmp/' + sHash + sExtension;
                            fsX.moveFile(sTempFile, sFinalFile, function() {
                                syslog.timeStop(iStart, {action: 'KnoxedUp._toTemp.done', hash: sHash, file: sFinalFile});
                                fCallback(sFinalFile, sHash);
                            });
                        });
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
            fsX.hashFile(this.getLocalPath(sFile), function(oError, sHash) {
                fCallback(sHash);
            });
        } else {
            this.toTemp(sFile, sType, function(sTempFile) {
                fsX.hashFile(sTempFile, function(oError, sHash) {
                    fs.unlink(sTempFile, function() {
                        fCallback(sHash);
                    });
                });
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
            this.toSha1(sFile, sType, function(sHash) {
                oTempFiles[sFile] = sHash;
                fCallbackAsync(null);
            }.bind(this))
        }.bind(this), function(oError) {
            fCallback(oError, oTempFiles);
        }.bind(this));
    };

    /**
     *
     * @param {Array}    aFiles    Array if file paths to download to temp
     * @param {String}   sType     Binary or (?)
     * @param {Function} fCallback Object of Temp Files with S3 file names as Key
     */
    KnoxedUp.prototype.filesToTemp = function(aFiles, sType, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var oTempFiles = {};
        async.forEach(aFiles, function (sFile, fCallbackAsync) {
            this.toTemp(sFile, sType, function(sTempFile) {
                oTempFiles[sFile] = sTempFile;
                fCallbackAsync(null);
            }.bind(this))
        }.bind(this), function(oError) {
            fCallback(oError, oTempFiles);
        }.bind(this));
    };

    /**
     *
     * @param {Array}    aFiles    Array if file paths to download to temp
     * @param {String}   sType     Binary or (?)
     * @param {String}   sExtension
     * @param {Function} fCallback Object of Temp Files with S3 file names as Key
     */
    KnoxedUp.prototype.filesToTempWithExtension = function(aFiles, sType, sExtension, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var oTempFiles = {};
        async.forEach(aFiles, function (sFile, fCallbackAsync) {
            this.toTemp(sFile, sType, sExtension, function(sTempFile) {
                oTempFiles[sFile] = sTempFile;
                fCallbackAsync(null);
            }.bind(this))
        }.bind(this), function(oError) {
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
