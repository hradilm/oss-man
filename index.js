const fs = require('fs');
const ForgeOss = require('./forge-oss');
const inquirer = require('inquirer');
const settings = require('./settings.json');
const activeSetttins = settings[settings.active];

const dataFolderName = 'data';

async function getFiles(path) {
    const entries = await fs.readdirSync(path, { withFileTypes: true });

    const files = entries
        .filter(file => !file.isDirectory())
        .map(file => ({ ...file, path: path + file.name }));

    const folders = entries.filter(folder => folder.isDirectory());

    for (const folder of folders)
        files.push(...await getFiles(`${path}${folder.name}/`));

    return files;
}

async function chooseBuckets(buckets) {
    var choices = ['All buckets'];
    choices.push(...buckets);

    const bucketQuestions = [{
        type: 'checkbox',
        choices: choices, 
        name: 'BucketKey',
        message: 'BucketKey: '
    }];

    const bucketAnswers = await inquirer.prompt(bucketQuestions);

    if (bucketAnswers.BucketKey[0] === 'All buckets')
        bucketAnswers.BucketKey = buckets;

    return bucketAnswers.BucketKey;
}

async function main() {
    const oss = new ForgeOss();

    const actionQuestions = [
        {
            type: 'list', 
            choices: ['Download', 'Upload'], 
            name: 'Action',
            message: 'Action: '
        }
    ];

    const action = await inquirer.prompt(actionQuestions);

    console.log('Authenticate to OSS...');
    await oss.auth(activeSetttins.client_id, activeSetttins.client_secret);

    if (action.Action === 'Download')
    {
        const buckets = await oss.getBuckets();
        const selectedBuckets = await chooseBuckets(buckets);

        for (bucketKeyIndex in selectedBuckets) {
            const bucketKey = selectedBuckets[bucketKeyIndex];
            console.log(`Downloading data from bucket ${bucketKey}`);

            var objects = await oss.getObjects(bucketKey);

            for (objectIndex in objects) {
                const object = objects[objectIndex];
                console.log(`Getting object ${object}`);
                await oss.getObject(bucketKey, object, `${dataFolderName}/${bucketKey}/${object}`);
            }
        }
    }

    if (action.Action === 'Upload') {
        const buckets = fs.readdirSync('data');
        const selectedBuckets = await chooseBuckets(buckets);
        
        for (bucketIndex in selectedBuckets) {
            const bucketKey = selectedBuckets[bucketIndex];

            const files = await getFiles(`${dataFolderName}/${bucketKey}/`);

            console.log(`Creating bucket ${bucketKey}`);
            oss.createBucket(bucketKey);

            for (fileIndex in files) {
                const file = files[fileIndex];
                const filePath = file.path;
                const objectKey = filePath.replace(`${dataFolderName}/${bucketKey}/`, '');

                console.log(`Uploading object ${objectKey}`);
                await oss.putObject(bucketKey, objectKey, filePath);
            }
        }
    }

    console.log('Done.');
}

main();