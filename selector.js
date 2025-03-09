const prompts = require('prompts');
const { spawn } = require('child_process');

const districts = [
    'ba-dinh',
    'hoan-kiem',
    'tay-ho',
    'cau-giay',
    'dong-da',
    'hai-ba-trung',
    'thanh-xuan',
    'hoang-mai',
    'long-bien',
    'nam-tu-liem',
    'bac-tu-liem',
    'ha-dong'
];
async function main() {
    try {
        const { scrape } = await prompts({
            type: 'confirm',
            name: 'scrape',
            message: 'Do you want to start scraping?',
            initial: true
        });

        let args = []; // Arguments to pass to main.py

        if (scrape) {
            const districtOptions = [
                { title: 'all', value: 'all' },
                ...districts.map(district => ({ title: district, value: district }))
            ];

            const { selectedDistricts } = await prompts({
                type: 'multiselect',
                name: 'selectedDistricts',
                message: 'Select the districts you want to scrape (use space to select, enter to confirm):',
                choices: districtOptions,
                hint: '- Space to toggle, Enter to submit',
                instructions: '\nInstructions:\n' +
                              '    ↑/↓: Highlight option\n' +
                              '    ←/→/[space]: Toggle selection\n' +
                              '    a: Toggle all\n' +
                              '    enter/return: Complete answer',
                validate: value => value.length > 0 ? true : 'At least one district must be selected'
            });

            let districtsToScrape;
            if (selectedDistricts.includes('all')) {
                districtsToScrape = districts;
                console.log(`Selected all ${districts.length} districts: ${districts.join(', ')}`);
            } else {
                districtsToScrape = selectedDistricts;
                console.log(`Selected districts: ${districtsToScrape.join(', ')}`);
            }
            args.push(...districtsToScrape); // Add districts to args
        } else {
            console.log("Scraping skipped.");
        }

        // Prompt for parsing
        const { parse } = await prompts({
            type: 'confirm',
            name: 'parse',
            message: 'Do you want to start parsing?',
            initial: true
        });
        args.push(parse ? '--parse' : '--no-parse');

        // Prompt for prediction model
        const { buildModel } = await prompts({
            type: 'confirm',
            name: 'buildModel',
            message: 'Do you want to build prediction model?',
            initial: true
        });
        args.push(buildModel ? '--build-model' : '--no-build-model');

        console.log("Starting main.py with args:", args);
        const mainProcess = spawn('python', ['main.py', ...args]);

        mainProcess.stdout.on('data', (data) => {
            console.log(data.toString());
        });

        mainProcess.stderr.on('data', (data) => {
            console.error(`Error: ${data.toString()}`);
        });

        mainProcess.on('close', (code) => {
            console.log(`main.py process exited with code ${code}`);
            process.exit(code);
        });
    } catch (error) {
        console.error(`An unexpected error occurred: ${error.message}`);
        process.exit(1);
    }
}

main();