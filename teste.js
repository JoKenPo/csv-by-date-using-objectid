const { ObjectId } = require('bson');

const objectId = new ObjectId('5e04ca62d950a42a441fa838');
const date = objectId.getTimestamp();
const month = date.toISOString().substring(0, 7); 

console.log('date: ', date)
console.log('month: ', month)