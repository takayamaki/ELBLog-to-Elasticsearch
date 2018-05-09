const AWS = require('aws-sdk');
const credentials = new AWS.EnvironmentCredentials('AWS');
const LineStream = require('byline').LineStream;
const path = require('path');
const stream = require('stream');
const createGunzip = require('zlib').createGunzip;

const config = {
    endpoint: process.env.ELASTIC_HOST,
    region: process.env.REGION,
    index: process.env.INDEX_NAME,
    doctype: process.env.DOC_TYPE,
    timeoffset: Number(process.env.TIME_OFFSET)
};
const endpoint =  new AWS.Endpoint(config.endpoint);
const s3 = new AWS.S3();
var totalLines = 0;
var addedDocs = 0;

function s3LogsToES(context, bucket, key, unzipStream, lineStream, recordStream) {
    const s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();

    s3Stream
      .pipe(unzipStream)
      .pipe(lineStream)
      .pipe(recordStream)
      .on('data', (parsedEntry) => {
          postDocumentToES(parsedEntry, context);
      });

    s3Stream.on('error', () => {
        console.log(
            'Error raised when getting object "' + key + '" from bucket "' + bucket + '".');
        context.fail();
    });
}

function postDocumentToES(doc, context) {
    const req = new AWS.HttpRequest(endpoint);
    
    indexName = `${config.index}-${convertTimezone(new Date(),config.timeoffset,"YYYYMM")}`;

    req.method = 'POST';
    req.path = path.join('/', indexName, config.doctype);
    req.region = config.region;
    req.body = doc;
    req.headers['presigned-expires'] = false;
    req.headers.Host = endpoint.host;

    const signer = new AWS.Signers.V4(req, 'es');
    signer.addAuthorization(credentials, new Date());

    const sender = new AWS.NodeHttpClient();
    sender.handleRequest(req, null, (httpResp) => {
        let body = '';
        httpResp.on('data', function (chunk) {
            body += chunk;
        });
        httpResp.on('end', function (chunk) {
            addedDocs ++;
            if (addedDocs === totalLines) {
                // Mark lambda success.  If not done so, it will be retried.
                console.log('All ' + addedDocs + ' log records added to ES.');
                context.succeed();
            }
        });
    }, (err) => {
        console.log('Error: ' + err);
        console.log(addedDocs + 'of ' + totalLines + ' log records added to ES.');
        context.fail();
    });
}

function splitLineWithoutEscape(line) {
    let stack = [];
    let inEscape = false;
    line.split(' ').forEach((elm) => {
        if (inEscape) {
            if (elm.match(/\"$/)){
                inEscape = false;
                stack.push(stack.pop() + ' ' + elm.replace(/\"$/,''));
            }else{
                stack.push(stack.pop() + ' ' + elm);
            }
         }else{
            if (elm.match(/^\"/)){
                inEscape = true;
                stack.push(elm.replace(/^\"/,''));
    
                if (elm.match(/\"$/)){
                   inEscape = false;
                    stack.push(stack.pop().replace(/\"$/,''));
                }
    
            }else{
                stack.push(elm);
            }
         }
        }
    );
    return(stack);
}

function pad(number) {
    if (number < 10) {
      return '0' + number;
    }
    return number;
}

function convertTimezone(str, offset, format = "YYYY-MM-DDTHH:mm:SSOOOOOO"){
    const utc = new Date(String(str));
    const dst = new Date(utc - ((0 - offset * 60)  - new Date().getTimezoneOffset()) * 60000);

    let result = format;
    const year = dst.getFullYear();
    const month = pad(dst.getMonth() + 1);
    const date = pad(dst.getDate());
    const hour = pad(dst.getHours());
    const min = pad(dst.getMinutes());
    const sec = pad(dst.getSeconds());

    let offsetStr;
    let offsetHour;
    let offsetMin;
    if (offset > 0){
        offsetHour = `${pad(Math.floor(offset))}`;

        offsetMin = String(offset).split(".")[1];
        if (offsetMin){
            offsetMin = 6 * offsetMin / 10;
        }else{
            offsetMin ="00"
        }

        offsetStr = `+${offsetHour}:${offsetMin}`;
    }else if(offset === 0){
        offsetStr = 'Z';
    }else if(offset < 0){
        offsetHour = `${pad(Math.floor(-offset))}`;

        offsetMin = String(offset).split(".")[1];
        if (offsetMin){
            offsetMin = 6 * offsetMin / 10;
        }else{
            offsetMin ="00"
        }

        offsetStr = `-${offsetHour}:${offsetMin}`;
    }else{
        return null;
    }

    result = result.replace(/YYYY/,year);
    result = result.replace(/MM/,month);
    result = result.replace(/DD/,date);
    result = result.replace(/HH/,hour);
    result = result.replace(/mm/,min);
    result = result.replace(/SS/,sec);
    result = result.replace(/OOOOOO/,offsetStr);
    return result;
}

function parseALBLog(line){
    const splited = splitLineWithoutEscape(line);

    const parsedLog = {
        'scheme': splited[0],
        'time': convertTimezone(splited[1],config.timeoffset),
        'elbName': splited[2],
        'srcIP': splited[3].split(':')[0],
        'target': splited[4],
        'requestProcessingTime': Number(splited[5]),
        'targetProcessingTime': Number(splited[6]),
        'responseProcessingTime': Number(splited[7]),
        'elbStatusCode': Number(splited[8]),
        'targetStatusCode': Number(splited[9]),
        'receivedBytes': Number(splited[10]),
        'sentBytes': Number(splited[11]),
        'requestPath': splited[12],
        'userAgent': splited[13],
        'sslCipher': splited[14],
        'sslProtocol': splited[15],
        'targetGroupArn': splited[16],
        'traceID': splited[17],
        'domainName': splited[18],
        'chosenCertArn': splited[19]
    };
    return parsedLog;
}

exports.handler = (event, context) => {
    const lineStream = new LineStream();
    const recordStream = new stream.Transform({objectMode: true});
    recordStream._transform = function(line, encoding, done) {
        const logRecord = parseALBLog(line.toString());
        const serializedRecord = JSON.stringify(logRecord);
        this.push(serializedRecord);
        totalLines ++;
        done();
    };
    const unzipStream = new createGunzip();

    event.Records.forEach((record) => {
        const bucket = record.s3.bucket.name;
        const objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
        s3LogsToES(context, bucket, objKey, unzipStream, lineStream, recordStream);
    });
};
