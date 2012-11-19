try {

var KnoxedUp    = require('../knoxed-up');
var exec        = require('child_process').exec;
var fs          = require('fs');
var fsX         = require('fs-extended');
var oConfig     = require('/etc/cameo/.config.js');
var crypto      = require('crypto');

var arguments = process.argv.splice(2);
var sHashes  = arguments

var s3 = new KnoxedUp(oConfig);

KnoxedUp.prototype.onProgress = function(oProgress) {
    process.stdout.write("\r" + oProgress.percent + '%');
}

var getPath = function(sHash) {
    return sHash.substr(0, 1) + '/' + sHash.substr(1, 1) + '/' + sHash.substr(2, 1) + '/' + sHash;
};

var i = 0;
var n = 1;

var go = function(i,n,sHash) {
    var sFrom = '/tmp/' + sHash;
    var sTo = getPath(sHash);
    
    var md5 = crypto.createHash('md5');
    

    fsX.md5FileToBase64(sFrom, function(oError, digest) {   
        console.log('md5base64 hash',digest); 

    // var stream = fs.createReadStream( sFrom, { encoding:'binary' });
    
    // stream.addListener('data', function(chunk) {
    //     md5.update(chunk);
    // });

    // stream.addListener('close', function() {
    //     var digest = md5.digest('base64');

        var headers = { "Content-Type" : ["video/mp4"] ,
                        "Content-MD5"  : digest };

        // rm file if it exists        
        s3.deleteFile(sTo, function(oError) {
            if (oError) {
    	       console.log('test_upload deleteFile caught error',oError);
            }
            else {
                s3.putStream(sFrom, sTo, headers, function(oError) {
                    if (oError) {
                        console.log('putStream caught error',oError);
                    } else {
                        i++;

                        console.log('putStream succeeded trial',i,'sha1hash',sHash,'md5sum',digest);
                        if (i < n) {
                            go(i,n,sHash);
                        }          
                    }
                }.bind(this));
    	    }
        }.bind(this));
    }.bind(this));
}.bind(this);


for (var iHash=0;iHash < sHashes.length;iHash++) {
        go(i,n,sHashes[iHash]);
}

}
catch (e) {
    console.log('error',e);
}
