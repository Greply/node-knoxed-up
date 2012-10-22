    var KnoxedUp = require("../knoxed-up");
    var fs       = require('fs');
    var fsX      = require('fs-extended');
    var testCase = require('nodeunit').testCase;
    var async    = require('async');
    var syslog   = require('syslog-console').init('KnoxedUp');
    require('longjohn');

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

            S3.toTemp(sPath, 'binary', function(oError, sTempFile, sHash) {
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

            S3.toTemp(sPath, 'binary', '.avi', function(oError, sTempFile, sHash) {
                test.equal(sHash,     sFileHash,          "Downloaded file has Correct Hash");
                test.equal(sTempFile, '/tmp/' + sFileHash + '.avi', "Temp file is Named Correctly");

                fs.exists(sTempFile, function(bExists) {
                    test.ok(bExists, "File Exists in /tmp");
                    test.done();
                });
            });
        }

        /*
        "Socket Hangup": function(test) {
            test.expect(1);
            test.ok(S3 instanceof KnoxedUp, "Instance created");

            var aFiles = [
                // Thumbnails
                "0063eca2902e404c231e7c1d2eedb4f4506528a8",
                "007e2ae89e23a9cc5e4128c65e193e7d40d68ab4",
                "023545d990c63b55c151630252fbbbd017f4a8bc",
                "027ddebfebb03be9dccd2365cc211a467297b8c5",
                "028daded69b30f74c1572cf2f7b04c0be7ce0286",
                "029c3545c2ead0111ddbb596dceababa09753e30",
                "03fd9a5438f7e87a5eca769140fd87775b442f53",
                "0445d4dbdd82d6d5c27ef8170bf81129bc562ebc",
                "04fa8a95a2b8b8a434bd038a30b3903de0ad0765",
                "050fb4b2f78b66c7f6f48d23e9e08b7fbaec421b",
                "0574e4c4239adc397abc75cb7d3a946536bd6752",
                "063b46d4de390d257eac0db0075702d1406bb0f8",
                "0680257268db35023234c455b6529c7d50d95120",
                "06b7b2b796c92a260c2fc0319a6376f32646e8ab",
                "06cffa9306e45986a38b71bce396c60efa8918af",
                "06e0ef10822f88b0869ea44c339801fdc9701cf5",
                "07e92f0287b8bd0c0f67e0672266e56eb655118f",
                "0810438e1328166a4c2322b05065df100cbd7695",
                "0815049ebefb89acf715d689b940bde9b00c9436",
                "089db88c35ba3ff6296672e22ddea22915e288a2",
                "0942eb6e54483f43b2ec811005aa3f7277ceec6b",
                "094d0e4b5cbe9345ec677cf887726936fe92b849",
                "0967722963847e542f8932a98312361cd567f970",
                "09f19309897902adfdfc5e4909d45989b08508e2",
                "0a36255385b8fc636673a7d64ea9004c14845bfb",
                "0a504fb774c53d41a3f2e1c5f2b8785038aba5f8",
                "0a7243c0f1b5989d5a9a6e92e6f042724d7fd8e4",

                // WAVs
                "02fd663763b580a84746c99291a26b0492e7a023",
                "042476a4d4be607fb1c6010138aa7c02f907f6c3",
                "06a1d8e2f80e7c51abebd1421210691c30ab8405",
                "06cfa3a68d5ea9c4052058501077c1a37042de13",
                "072c491951c0faa188fd56b736010723cf448572",
                "07a3293b8afe21c977bbccef2fcc83eb0138a081",
                "087aa558129db2cb572f7b5b0e1494cdbd546a9b",
                "0966f69b896a7af31a622df0034011619c9d0aed",
                "0de8c3bcdd72eb7c020437acf34ed45020b4570b"
            ];

            function shuffle(array) {
                var tmp, current, top = array.length;

                if(top) while(--top) {
                    current = Math.floor(Math.random() * (top + 1));
                    tmp = array[current];
                    array[current] = array[top];
                    array[top] = tmp;
                }

                return array;
            }

            aFiles = shuffle(aFiles);

            for (var i in aFiles) {
                var aPath = aFiles[i].split('').slice(0, 3);
                aFiles[i] = aPath.join('/') + '/' + aFiles[i];
            }

            var iTimes = 0;
            var iUntil = 5;
            var dl = function() {
                iTimes++;
                S3.filesToTempWithExtension(aFiles, 'binary', '.png', function(oError, oTempFiles) {
                    if (oError) {
                        console.error('ERROR', oError);
                        test.ok(false, "Retry Failed");
                        test.done();
                    } else {
                        console.log(oTempFiles);
                        var aTempFiles = [];
                        for (var sFile in oTempFiles) {
                            aTempFiles.push(oTempFiles[sFile]);
                        }

                        async.forEach(aTempFiles, fs.unlink, function() {
                            if (iTimes >= iUntil) {
                                test.ok(true, "Downloaded Properly");
                                test.done();
                            } else {
                                process.nextTick(dl);
                            }
                        });
                    }
                }.bind(this));
            };

            dl();
        }
        */
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