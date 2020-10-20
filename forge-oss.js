const fs = require('fs');
const fetch = require('node-fetch');
const path = require('path');
const mkdirp = require('mkdirp');
const util = require('util');
const streamPipeline = util.promisify(require('stream').pipeline);

module.exports = class ForgeOss {
    constructor() {
        this.bearer = null;
        this.baseUrl = 'https://developer.api.autodesk.com';
    }

    async auth(client_id, client_secret) {
        const params = new URLSearchParams();
        params.append('client_id', client_id);
        params.append('client_secret', client_secret);
        params.append('grant_type', 'client_credentials');
        params.append('scope', 'bucket:read data:read bucket:create data:write data:create');
    
        const response = await fetch(`${this.baseUrl}/authentication/v1/authenticate`, { 
            method: 'post', 
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params
        });
    
        const responseJson = await response.json();
        this.bearer = responseJson.access_token;        
    }

    async getPagedOssItem(urlParam, getItemFunc) {
        var nextStartAt = null;
        var allItemKeys = [];
        do {
            const params = new URLSearchParams(nextStartAt);
            const url = `${this.baseUrl}/${urlParam}?${params}`;
            const response = await fetch(url, { 
                method: 'get', 
                headers: { 
                    'Authorization': 'Bearer ' + this.bearer,
                    'Content-Type': 'application/json'
                }
            });
        
            const responseJson = await response.json();

            const itemKeys = responseJson.items.map(item => getItemFunc(item))

            allItemKeys.push(...itemKeys);
    
            nextStartAt = null;
            if (responseJson.next !== undefined) {
                const nextUrl = new URL(responseJson.next)
                nextStartAt = nextUrl.search;
            }

        } while(nextStartAt !== null);

        allItemKeys.sort();
        return allItemKeys;
    }

    async getBuckets() {
        return await this.getPagedOssItem(`oss/v2/buckets`, function(item) {return item.bucketKey});
    }

    async getObjects(bucketKey) {
        return await this.getPagedOssItem(`oss/v2/buckets/${bucketKey}/objects`, function (item) {return item.objectKey});
    }

    async getObject(bucketKey, objectPath, localPath) {
        const objectKey = encodeURIComponent(objectPath);
        const response = await fetch(`${this.baseUrl}/oss/v2/buckets/${bucketKey}/objects/${objectKey}`, { 
            method: 'get', 
            headers: { 
                'Authorization': 'Bearer ' + this.bearer,
            }
        });
    
        const dir = path.dirname(localPath);
        mkdirp.sync(dir);

        const file = fs.createWriteStream(localPath);
        await streamPipeline(response.body, file);
    }

    async putObject(bucketKey, objectPath, localPath) {
        const objectKey = encodeURIComponent(objectPath);
        const stats = fs.statSync(localPath);
        const fileSizeInBytes = stats.size;      
        let readStream = fs.createReadStream(localPath);

        const response = await fetch(`${this.baseUrl}/oss/v2/buckets/${bucketKey}/objects/${objectKey}`, { 
            method: 'put', 
            headers: { 
                'Authorization': 'Bearer ' + this.bearer,
                "Content-length": fileSizeInBytes
            },
            body: readStream
        });

        const output = await response.text();
    }

    async createBucket(bucketKey) {
        const bodyText = `{ "bucketKey": "${bucketKey}", "policyKey": "persistent"}`;
        
        const response = await fetch(`${this.baseUrl}/oss/v2/buckets`, { 
            method: 'post', 
            headers: { 
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + this.bearer
            },
            body: bodyText
        });

        const responseJson = await response.json();       
        return responseJson; 
    }
}
