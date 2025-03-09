const prompts = require('prompts');
const { spawn } = require('child_process');

// List of districts (must match main.py's districts)
const districts = [
    "hoan-kiem",
    "thanh-xuan",
    "cau-giay",
    "nam-tu-liem",
    "bac-tu-liem",
    "hai-ba-trung",
    "dong-da",
    "ha-dong",
    "hoang-mai",
    "long-bien",
    "tay-ho",
    "ba-dinh",
];
async function main() {
    try {
        const { scrape } = await prompts({
            type: 'confirm',
            name: 'scrape',
            message: 'Do you want to start scraping?',
            initial: true
        });

        if (scrape) {
            // Define district options including "all"
            const districtOptions = [
                { title: 'all', value: 'all' },
                ...districts.map(district => ({ title: district, value: district }))
            ];

            const { selectedDistricts } = await prompts({
                type: 'multiselect',
                name: 'selectedDistricts',
                message: 'Select the districts you want to scrape (use space to select, enter to confirm):',
                choices: districtOptions,
                hint: '- Space to toggle, Enter to submit', // Matches your example
                instructions: '\nInstructions:\n' +
                              '    ↑/↓: Highlight option\n' +
                              '    ←/→/[space]: Toggle selection\n' +
                              '    a: Toggle all\n' +
                              '    enter/return: Complete answer', // Explicit instructions
                validate: value => value.length > 0 ? true : 'At least one district must be selected'
            });

            // Handle the selection
            let districtsToScrape;
            if (selectedDistricts.includes('all')) {
                districtsToScrape = districts;
                console.log(`Selected all ${districts.length} districts: ${districts.join(', ')}`);
            } else {
                districtsToScrape = selectedDistricts;
                console.log(`Selected districts: ${districtsToScrape.join(', ')}`);
            }

            // Run main.py with selected districts as arguments
            console.log("Starting main.py...");
            const mainProcess = spawn('python', ['main.py', ...districtsToScrape]);

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
        } else {
            console.log("Scraping skipped. Starting main.py without scraping...");
            const mainProcess = spawn('python', ['main.py']);
            mainProcess.stdout.on('data', (data) => console.log(data.toString()));
            mainProcess.stderr.on('data', (data) => console.error(data.toString()));
            mainProcess.on('close', (code) => {
                console.log(`main.py process exited with code ${code}`);
                process.exit(code);
            });
        }
    } catch (error) {
        console.error(`An unexpected error occurred: ${error.message}`);
        process.exit(1);
    }
}

main();