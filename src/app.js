const express = require('express');
var mongoose = require('mongoose');
const bodyParser = require('body-parser');
const busboyBodyParser = require('busboy-body-parser');
const app = express();
const fs = require('fs');
const port = 3232;
const root = '/api';
var path = require('path');
// const config = require('../config/defualt');
// const { port, root } = config.get('api');
const mysql = require('mysql2');
app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})


var destPath = __dirname + "\\public\\";
app.use(express.static(path.join(__dirname, 'public')));
// console.log("destPath---",destPath)
// const { port, root } = config.get('api');

//including models files for any os.
var modelsdir= path.normalize(__dirname+'/app/models/');
fs.readdirSync(modelsdir).forEach(function(file){
  if(file.indexOf(".js")){
    require(modelsdir+file);
  }
});

app.use(bodyParser.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  next();
});
app.use(busboyBodyParser({ limit: '100mb' }));


const connection2 = mysql.createConnection({
    host: '13.235.164.250', // host for connection
    port: '3306', // default port for mysql is 3306
    database: 'system', // database from which we want to connect out node application
    user: 'root', // username of the mysql connection
    password: '8Lq6DZRkX5niM8aRbDeh' // password of the mysql connection
    });
  
   connection2.connect(function (err) {
      if(err){
          console.log("error occured while connecting");
      }
      else{
          console.log("connection created with Mysql successfully");
      }
   });

//parsing middlewares
app.use(bodyParser.json({limit:'50mb',extended:true}));
app.use(bodyParser.urlencoded({limit:'100mb',extended:true}));

app.get('/', (req, res) => {
    res.send('Hello World!')
});

const report = require('./api/routes/report');

// routes for common controllers
app.use(`/report`,report);

// module.exports = app;