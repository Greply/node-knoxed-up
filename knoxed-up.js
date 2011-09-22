    var fs          = require('fs');
    var temp        = require('temp');
    var Knox        = require('knox');

    var KnoxedUp = function(config) {
        this.oClient = Knox.createClient(config);
    };

    module.exports = KnoxedUp;

    KnoxedUp.prototype.getFileList = function(sPrefix, fCallback, fError) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};
        fError    = typeof fError    == 'function' ? fError     : function() {};

        this.oClient.get('/?prefix=' + sPrefix).on('response', function(oResponse) {
            oResponse.setEncoding('utf8');
            oResponse.on('data', function(oChunk){
                parser(oChunk, function (oError, oResult) {
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

    KnoxedUp.prototype.getFile = function(sFile, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        this.oClient.get(sFile).on('response', function(oResponse) {
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
    };

    KnoxedUp.prototype.moveFile = function(sFrom, sTo, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};
        
        var oOptions = {
            'Content-Length': '0',
            'x-amz-copy-source': '/' + this.oClient.bucket + '/' + sFrom,
            'x-amz-metadata-directive': 'COPY'
        };

        this.oClient.put(sTo, oOptions).on('response', function(oResponse) {
            console.log(oResponse.statusCode);
            console.log(oResponse.headers);
            oResponse.setEncoding('utf8');
            oResponse.on('data', function(oChunk){
                console.log(oChunk);
                this.oClient.del(sFrom).end();
                fCallback(oChunk);
            });
        }).end();
    };

    KnoxedUp.prototype.toTemp = function(sFile, sType, fCallback) {
        fCallback = typeof fCallback == 'function' ? fCallback  : function() {};

        var sType     = sType || 'binary';
        var sTempFile = temp.path();
        var sContents = '';

        this.oClient.getFile(sFile, function(error, oResponse) {
            oResponse.setEncoding(sType);
            oResponse
                .on('data', function(sChunk) {
                    this.sContents += sChunk;
                })
                .on('end',  function() {
                    fs.writeFile(sTempFile, sContents, sType, function(oError) {
                        fs.chmod(sTempFile, '777', function() {
                            fs.chown(sTempFile, 1000, 1000, function() {
                                fCallback(sTempFile);
                            });
                        });
                    });
                });
        });
    };