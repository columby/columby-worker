/* here we go */


var kue = require('kue');
kue.createQueue();
kue.app.set('title', 'Columby Worker');
kue.app.listen(3000);
