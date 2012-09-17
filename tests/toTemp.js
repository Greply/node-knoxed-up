    var KnoxedUp = require ('../knoxed-up');

    var S3 = new KnoxedUp({
        key:    'AKIAJ7CBLVZ2DSXOOBWQ',
        secret: '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',
        bucket: 'media.cameoapp.com'
    });

    S3.toTemp('6/2/2/622a9a4e89093bbe6f03f68c6c277e6ce5af8d0b', 'binary', '.avi', function(sTempFile, sHash) {
        console.log(sTempFile);
        console.log(sHash);
    });