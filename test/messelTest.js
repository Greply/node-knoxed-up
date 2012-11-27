var KnoxedUp    = require('../knoxed-up');
var exec        = require('child_process').exec;
var fs          = require('fs');
var fsX         = require('fs-extended');
var oConfig     = require('/etc/cameo/.config.js');

var arguments = process.argv.splice(2);
var sHashes  = arguments

var s3 = new KnoxedUp(oConfig);

var getPath = function(sHash) {
    return sHash.substr(0, 1) + '/' + sHash.substr(1, 1) + '/' + sHash.substr(2, 1) + '/' + sHash;
};

var i = 0;
var n = 10;

var go = function(i,n,sHash) {
    var sFrom = getPath(sHash);
    // rm file if it exists
    var fname = fsX.getTmpSync() + sHash;
    if (fs.existsSync(fname)) fs.unlinkSync(fname);

    s3.toTemp(sFrom, 'binary', sHash, function(oError, sTempFile, sHash) {
        if (oError) {
            console.log('KnoxedUp toTemp caught error',oError);
        } else {
            i++;
            // console.log('finished downloading file, about to take sha1 sum of downloaded file');
            exec('sha1sum ' + sTempFile, function(oError, sSTDOut, sSTDError) {
                if (oError) {
                    console.error('sha1sum Error', oError);
                } else {
                    var aHash = sSTDOut.split(' ');
                    console.log('return hash (',aHash[0],') original ', sHash);
                    if (aHash[0] !== sHash) {
                        console.log('error with trial, sha1sum mismatch (',aHash[0],')',sHash)
                    }
                    else {
                        console.log('matched trial',i,'hashes',aHash[0], sHash);
                        if (i < n) {
                            go(i,n,sHash);
                        }
                    }
                }
            });
        }
    });
}.bind(this);


for (var iHash=0;iHash < sHashes.length;iHash++) {
        go(i,n,sHashes[iHash]);
}
