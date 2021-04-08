const { Pool, Client } = require('pg')
require('dotenv').config()
const AWS = require('aws-sdk')

AWS.config.update({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});


const pool = new Pool()

pool.query('SELECT '+
              'c.id as id_contract, '+
              'CASE '+
                'WHEN c.titular_name is null '+
                'THEN c.client_name '+
                'ELSE c.titular_name '+
              'END as nome, '+
              'CASE '+
                'WHEN c.titular_cpf is null '+
                'THEN regexp_replace(c.client_cpf, \'[^0-9]+\', \'\',\'g\')  '+
                'ELSE regexp_replace(c.titular_cpf, \'[^0-9]+\', \'\',\'g\') '+
              'END as cpf, '+
              's.letter as uf '+
              'from contracts c '+
              'INNER JOIN states s '+
              'ON s.id = c.vehicle_licensing_state_id '+
              'WHERE '+
              'c.judicial_process_exists = false', (err, res) => {

  //console.log(res);
  res.rows.forEach(function(row){

    var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

    var params = {
      MessageBody: JSON.stringify({"cpf": row.cpf, "nome": row.nome, "uf": row.uf, "contract_id": row.id_contract}),
      QueueUrl: process.env.AWS_QUEUE_LOCALIZACAO
    };
    
    sqs.sendMessage(params, function(err, data) {
      if (err) {
        console.log("Error " + err);
      } else {
        console.log("Enviando SQS processos: " + data.MessageId);
      }
    });

  });

  pool.end()
});
