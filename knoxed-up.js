    var fs          = require('fs');
    var path        = require('path');
    var temp        = require('temp');
    var Knox        = require('knox');
    var fs_tools    = require('fs-extended');
    var xml2js      = require('xml2js');
    var async       = require('async');
    var Buffer      = require('buffer').Buffer;

    var KnoxedUp = function(config) {
        this.Client = Knox.createClient(config);

        if (config.local !== undefined
        &&  config.path  !== undefined) {
            KnoxedUp.setLocal(config.local, config.path);
        }
    };

    module.exports = KnoxedUp;

    /**
     *
     * @param string   sPrefix   Path of folder to list
     * @param function fCallback Array of Objects in that folder
     * @param function fError
     */
    KnoxedUp.prototype.getFileList = function(sPrefix, fCallback, fError) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};
        fError    = typeof fError    == 'function' ? fError     : function() {};

        var parser = new xml2js.Parser();

        this.Client.get('/?prefix=' + sPrefix).on('response', function(oResponse) {
            var sContents = '';
            oResponse.setEncoding('utf8');
            oResponse
                .on('data', function(sChunk){
                    sContents += sChunk;
                })
                .on('end', function(sChunk) {
                    parser.parseString(sContents, function (oError, oResult) {
                        if (oError) {
                            fError(oError);
                        } else {
                            var aFiles = [];
                            if (oResult.Contents !== undefined) {
                                if (oResult.Contents.length) {
                                    for (var i in oResult.Contents) {
                                        if (oResult.Contents[i].Key) {
                                            aFiles.push(oResult.Contents[i].Key)
                                        }
                                    }
                                }
                            }

                            fCallback(aFiles);
                        }
                    });
                });
        }).end();
    };

    /**
     *
     * @param string sFile       Path to file
     * @param function fCallback boolean
     */
    KnoxedUp.prototype.fileExists = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            path.exists(KnoxedUp.sPath + sFile, fCallback);
        } else {
            this.Client.head(sFile).on('response', function(oResponse) {
                fCallback(oResponse.statusCode != 404);
            }).end();
        }
    };

    /**
     *
     * @param string   sFile     Path to File
     * @param function fCallback Full contents of File
     */
    KnoxedUp.prototype.putStream = function(sFrom, sTo, oHeaders, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            fs_tools.copyFile(sFrom, KnoxedUp.sPath + sTo, function() {
                fCallback(sTo);
            });
        } else {
            this.Client.putStream(fs.createReadStream(sFrom), sTo, oHeaders, function() {
                fCallback(sTo);
            });
        }
    };

    /**
     *
     * @param string   sFile     Path to File
     * @param function fCallback Full contents of File
     */
    KnoxedUp.prototype.getFile = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            fCallback(fs.readFileSync(KnoxedUp.sPath + sFile));
        } else {
            this.Client.get(sFile).on('response', function(oResponse) {
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
     * @param string   sFile     Path to File
     * @param string   sType     ascii, utf8, ics2, base64, binary
     * @param function fCallback Buffer containing full contents of File
     */
    KnoxedUp.prototype.getFileBuffer = function(sFile, sType, fDoneCallback, fBufferCallback) {
        fDoneCallback   = typeof fDoneCallback   == 'function' ? fDoneCallback    : function() {};
        fBufferCallback = typeof fBufferCallback == 'function' ? fBufferCallback  : function() {};
        sType           = sType || 'utf8';

        if (KnoxedUp.isLocal()) {
            fs.readFile(KnoxedUp.sPath + sFile, sType, function(oError, oBuffer) {
                console.log('Read', KnoxedUp.sPath + sFile, oBuffer.length);
                fDoneCallback(oBuffer);
            });

            return {
                abort: function() {
                    console.error('ABORT!');
                }
            }
        } else {
            var oRequest = this.Client.get(sFile);
            oRequest.on('response', function(oResponse) {
                var oBuffer  = new Buffer(parseInt(oResponse.headers['content-length'], 10));
                var iBuffer  = 0;
                var iWritten = 0;
                oResponse.setEncoding(sType);
                oResponse
                    .on('data', function(sChunk){
                        iWritten = oBuffer.write(sChunk, iBuffer, sType);
                        iBuffer += iWritten;
                        fBufferCallback(oBuffer, iBuffer, iWritten);
                    })
                    .on('end', function(){
                        fDoneCallback(oBuffer);
                    });
            }).end();
        }

        return oRequest;
    };

    /**
     *
     * @param array    aFiles    - Array of filenames to retrieve
     * @param function fCallback - Contents object with filename as key and file contents as value
     */
    KnoxedUp.prototype.getFiles = function(aFiles, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var oContents = {};
        var iContents = 0;
        var iFiles    = aFiles.length;
        if (iFiles) {
            for (var i in aFiles) {
                var sFile = aFiles[i];
                this.getFile(sFile, function(sContents) {
                    iContents++;
                    oContents[sFile] = sContents;

                    if (iContents >= iFiles) {
                        fCallback(oContents);
                    }
                })
            }
        }
    };

    /**
     *
     * @param string   sFrom     Path of File to Move
     * @param string   sTo       Destination Path of File
     * @param function fCallback
     */
    KnoxedUp.prototype.copyFile = function(sFrom, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            fs_tools.copyFile(KnoxedUp.sPath + sFrom, KnoxedUp.sPath + sTo, fCallback);
        } else {
            var oOptions = {
                'Content-Length': '0',
                'x-amz-copy-source': '/' + this.Client.bucket + '/' + sFrom,
                'x-amz-metadata-directive': 'COPY'
            };

            this.Client.put(sTo, oOptions).on('response', function(oResponse) {
                oResponse.setEncoding('utf8');
                oResponse.on('data', function(oChunk){
                    fCallback(oChunk);
                });
            }).end();
        }
    };

    /**
     *
     * @param string   sFrom     Path of File to Move
     * @param string   sTo       Destination Path of File
     * @param function fCallback
     */
    KnoxedUp.prototype.moveFile = function(sFrom, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        if (KnoxedUp.isLocal()) {
            fs_tools.moveFile(KnoxedUp.sPath + sFrom, KnoxedUp.sPath + sTo, fCallback);
        } else {
            this.copyFile(sFrom, sTo, function(oChunk) {
                this.Client.del(sFrom).end();
                fCallback(oChunk);
            });
        }
    };

    /**
     *
     * @param string   sFile     Path to File to Download
     * @param string   sType     Binary or (?)
     * @param function fCallback - Path of Temp File
     */
    KnoxedUp.prototype.toTemp = function(sFile, sType, oSettings, fCallback, fBufferCallback) {
        sType = sType || 'binary';

        var oDefault = {
            prefix: 'knoxed-',
            suffix: path.extname(sFile)
        };

        if (typeof oSettings == 'function') {
            fCallback = oSettings;
            oSettings = oDefault;
        } else {
            oSettings = oSettings || oDefault;
        }

        fCallback       = typeof fCallback       == 'function' ? fCallback         : function() {};
        fBufferCallback = typeof fBufferCallback == 'function' ? fBufferCallback  : function() {};

        temp.open(oSettings, function(oError, oTempFile) {
            if (KnoxedUp.isLocal()) {
                fs_tools.copyFile(KnoxedUp.sPath + sFile, oTempFile.path, function() {
                    fCallback(oTempFile.path);
                });
            } else {
                var oStream = fs.createWriteStream(oTempFile.path, {
                    flags: 'w',
                    encoding: sType,
                    mode: 0777
                });

                var oRequest = this.getFileBuffer(sFile, sType, function(oBuffer) {
                    oStream.end();

                    fs.chmod(oTempFile.path, 0777, function() {
                        fCallback(oTempFile.path, oBuffer);
                    });
                }, function(oBuffer, iLength, iWritten) {
                    oStream.write(oBuffer.slice(iLength - iWritten, iLength), sType);
                    fBufferCallback(oBuffer, iLength, iWritten);
                });

                return oRequest;
            }
        }.bind(this));
    };

    /**
     *
     * @param array    aFiles    Array if file paths to download to temp
     * @param string   sType     Binary or (?)
     * @param function fCallback Object of Temp Files with S3 file names as Key
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
