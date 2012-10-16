    var KnoxedUp = require("../knoxed-up");
    var fs       = require('fs');
    var fsX      = require('fs-extended');
    var testCase = require('nodeunit').testCase;
    var syslog   = require('syslog-console').init('KnoxedUp');

    syslog.disableTTY();

    // CREATE INSTANCE

    var S3 = new KnoxedUp({
        key:    'AKIAJ7CBLVZ2DSXOOBWQ',
        secret: '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',
        bucket: 'media.cameoapp.com'
    });

    var sFileHash = '62228dc488ce4a2619e460c117254db404981b1e';
    var aPath     = sFileHash.split('').slice(0, 3);
    var sPath     = aPath.join('/') + '/' + sFileHash;

    exports["Test Download To Temp"] = {
        tearDown: function (callback) {
            // clean up
            fsX.removeDirectory('/tmp/' + sFileHash, function() {
                fsX.removeDirectory('/tmp/' + sFileHash + '.avi', function() {
                    callback();
                });
            });
        },

        "To Temp": function(test) {
            test.expect(4);
            test.ok(S3 instanceof KnoxedUp, "Instance created");

            S3.toTemp(sPath, 'binary', function(sTempFile, sHash) {
                test.equal(sHash,     sFileHash,          "Downloaded file has Correct Hash");
                test.equal(sTempFile, '/tmp/' + sFileHash, "Temp file is Named Correctly");

                fs.exists(sTempFile, function(bExists) {
                    test.ok(bExists, "File Exists in /tmp");
                    test.done();
                });
            });
        },

        "To Temp With Extension": function(test) {
            test.expect(4);
            test.ok(S3 instanceof KnoxedUp, "Instance created");

            S3.toTemp(sPath, 'binary', '.avi', function(sTempFile, sHash) {
                test.equal(sHash,     sFileHash,          "Downloaded file has Correct Hash");
                test.equal(sTempFile, '/tmp/' + sFileHash + '.avi', "Temp file is Named Correctly");

                fs.exists(sTempFile, function(bExists) {
                    test.ok(bExists, "File Exists in /tmp");
                    test.done();
                });
            });
        }
    };

    exports["Update Headers"] = {
        "To Temp": function(test) {
            test.expect(1);

            var oHeaders = {
                'Content-Type': 'video/vnd.avi'
            };

            S3.updateHeaders('5/9/3/593949e21dee8eeb9c7af2f26b87f8bb0c2241c3', oHeaders, function(oUpdateError) {
                S3.getHeaders('5/9/3/593949e21dee8eeb9c7af2f26b87f8bb0c2241c3', function(oGetError, oGetHeaders) {
                    test.equal(oGetHeaders['content-type'], oHeaders['Content-Type'], 'Headers Updated Correctly');
                    test.done();
                });
            });
        }
    };