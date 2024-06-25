const fs = require('fs');
const csv = require('csv-parser');

const inputFile = 'input.csv'; // Nome do arquivo CSV de entrada
const outputFilePrefix = 'output'; // Prefixo para os arquivos de saída
const linesPerFile = 500000; // Número de linhas por arquivo

function splitCSV(inputFile, outputFilePrefix, linesPerFile) {
  let fileCount = 0;
  let lineCount = 0;
  let outputStream = createOutputStream(fileCount);

  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
      if (lineCount >= linesPerFile) {
        outputStream.end();
        fileCount += 1;
        lineCount = 0;
        outputStream = createOutputStream(fileCount);
      }
      outputStream.write(rowToCSV(row));
      lineCount += 1;
    })
    .on('end', () => {
      outputStream.end();
      console.log(`Arquivo CSV dividido em ${fileCount + 1} partes.`);
    });
}

function createOutputStream(fileCount) {
  return fs.createWriteStream(`output/${outputFilePrefix}_${fileCount + 1}.csv`);
}

function rowToCSV(row) {
  return Object.values(row).map(value => `"${value}"`).join(',') + '\n';
}

splitCSV(inputFile, outputFilePrefix, linesPerFile);
