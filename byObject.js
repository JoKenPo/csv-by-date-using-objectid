const fs = require('fs');
const csv = require('csv-parser');
const { ObjectId } = require('bson');

const inputFile = 'input.csv'; // Nome do arquivo CSV de entrada
const outputFilePrefix = 'output'; // Prefixo para os arquivos de saída

function splitCSVByMonth(inputFile, outputFilePrefix) {
  const outputStreams = {};

  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
      const objectId = new ObjectId(row._id); // Supondo que a coluna de ObjectId seja "_id"
      const date = objectId.getTimestamp();
      const month = date.toISOString().substring(0, 7); // Formato 'YYYY-MM'

      if (!outputStreams[month]) {
        outputStreams[month] = createOutputStream(outputFilePrefix, month);
      }

      outputStreams[month].write(rowToCSV(row));
    })
    .on('end', () => {
      for (const stream of Object.values(outputStreams)) {
        stream.end();
      }
      console.log(`Arquivo CSV dividido por meses.`);
    });
}

function createOutputStream(outputFilePrefix, month) {
  return fs.createWriteStream(`output/${outputFilePrefix}_${month}.csv`);
}

function rowToCSV(row) {
  return Object.values(row).map(value => `"${value}"`).join(',') + '\n';
}

splitCSVByMonth(inputFile, outputFilePrefix);
