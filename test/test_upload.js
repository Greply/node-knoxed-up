var config      = require('/etc/cameo/.config.js');
var KnoxedUp    = require('../knoxed-up');
var exec        = require('child_process').exec;
var fs          = require('fs');

var arguments = process.argv.splice(2);
var sHashes  = arguments

var s3 = new KnoxedUp(config.S3_MEDIA);

KnoxedUp.prototype.onProgress = function(oProgress) {
    process.stdout.write("\r" + oProgress.percent + '%');
}

var getPath = function(sHash) {
    return sHash.substr(0, 1) + '/' + sHash.substr(1, 1) + '/' + sHash.substr(2, 1) + '/' + sHash;
};

var i = 0;
var n = 10;

var go = function(i,n,sHash) {
    var sTo = getPath(sHash);
    var headers = { "Content-Type" : ["video/vnd.avi"] };
    // rm file if it exists
    var sFrom = '/tmp/' + sHash;
    s3.deleteFile(sTo, function(oError) {
        if (oError) {
	       console.log('KnoxdUp deleteFile caught error',oError);
        }
        else {
            s3.putStream(sFrom, sTo, headers, function(oError) {
                if (oError) {
                    console.log('KnoxedUp putStream caught error',oError);
                } else {
                    i++;

                    console.log('putStream succeeded trial',i,'hash',sHash);
                    if (i < n) {
                        go(i,n,sHash);
                    }
           
                }
            }.bind(this));
	    }
    }.bind(this));
      
   
}.bind(this);


for (var iHash=0;iHash < sHashes.length;iHash++) {
        go(i,n,sHashes[iHash]);
}
