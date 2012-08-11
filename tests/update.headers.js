    var KnoxedUp = require ('../knoxed-up');

    var S3 = new KnoxedUp({
        key:    'AKIAJ7CBLVZ2DSXOOBWQ',
        secret: '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',
        bucket: 'media.cameoapp.com'
    });

    var oHeaders = {
        'Content-Type': 'video/mp4'
    };

    S3.updateHeaders('5/9/3/593949e21dee8eeb9c7af2f26b87f8bb0c2241c3', oHeaders, function(sOutput) {
        console.log(sOutput);
        console.log(oHeaders);
    });