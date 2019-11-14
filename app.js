const express = require("express");
const mysql = require("mysql");
const axios = require('axios');
const app = express();
var _ = require('lodash');
app.use(express.json());

const pool = mysql.createPool({
    connectionLimit: 10,
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'vf'
});

pool.getConnection((err, connection) => {
    if (err) {
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            console.error('Database connection was closed.')
        }
        if (err.code === 'ER_CON_COUNT_ERROR') {
            console.error('Database has too many connections.')
        }
        if (err.code === 'ECONNREFUSED') {
            console.error('Database connection was refused.')
        }
    }    
    if (connection) connection.release()
    
    return
});

app.get('/loadmasters',(req, res)=>{
    axios.get('https://data.sfgov.org/resource/yitu-d5am.json')
    .then(function (response) {
        let data = response.data;
        let movies = _.uniq(_.map(data, 'title'))
        let years =_.map(_.uniq(_.map(data, 'release_year')),(a)=>{
            return [a];
        });
        let locations =_.map(_.uniq(_.map(data, 'locations')),(a)=>{
            return [a];
        });
        let directors =_.map(_.uniq(_.map(data, 'director')),(a)=>{
            return [a];
        });
        let writers =_.map(_.uniq(_.map(data, 'writer')),(a)=>{
            return [a];
        });
        let companies =_.map(_.uniq(_.map(data, 'production_company')),(a)=>{
            return [a];
        });
        let actors1 =_.uniq(_.map(data, 'actor_1'));
        let actors2 =_.uniq(_.map(data, 'actor_2'));
        let actors3 =_.uniq(_.map(data, 'actor_3'));
        let allactors =_.map(_.uniq([...actors1, ...actors2, ...actors3]),(a)=>{
            return [a];
        });

        console.log('data', allactors);
        pool.query('INSERT INTO actors (actor) VALUES ?', [allactors],(err, result)=>{
            console.log(err, result);
        });
        pool.query('INSERT INTO directors (director) VALUES ?', [directors],(err, result)=>{
            console.log(err, result);
        });
        pool.query('INSERT INTO locations (location) VALUES ?', [locations],(err, result)=>{
            console.log(err, result);
        });
        pool.query('INSERT INTO writers (writers) VALUES ?', [writers],(err, result)=>{
            console.log(err, result);
        });
        pool.query('INSERT INTO production_companies (company) VALUES ?', [companies],(err, result)=>{
            console.log(err, result);
        });
        pool.query('INSERT INTO years (year) VALUES ?', [years],(err, result)=>{
            console.log(err, result);
        });

        res.send('ok');
        
    })
    .catch(function (error) {
        //console.log(error);
    })
});



app.listen(3000);