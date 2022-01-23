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
              ' c.id as id_contract, '+  
              ' c.id as id_contract, '+
                ' CASE '+
                  ' WHEN c.titular_name is null '+
                  ' THEN c.client_name '+
                  ' ELSE c.titular_name '+
                ' END as nome, '+
                ' CASE '+
                  ' WHEN c.titular_cpf is null '+
                  ' THEN regexp_replace(c.client_cpf, \'[^0-9]+\', \'\',\'g\') '+
                  ' ELSE regexp_replace(c.titular_cpf, \'[^0-9]+\', \'\',\'g\') '+
                ' END as cpf, '+
            ' CASE '+
                  ' WHEN c.titular_cpf is null '+
                  ' THEN '+
            ' CASE '+
              ' WHEN sc.letter is null '+
              ' THEN sv.letter '+
              ' ELSE sc.letter '+
            ' END '+
                  ' ELSE '+
            ' CASE '+
              ' WHEN st.letter is null '+
              ' THEN sv.letter '+
              ' ELSE st.letter '+
            ' END '+
                ' END as uf_novo, '+
                ' sv.letter as uf '+
                ' from contracts c '+
                ' LEFT JOIN states sv '+
                ' ON sv.id = c.vehicle_licensing_state_id '+
            ' LEFT JOIN cities cc '+
                  ' ON cc.id = c.client_city_id '+
            ' LEFT JOIN states sc '+
                  ' ON sc.id = cc.state_id '+
            ' LEFT JOIN cities ct '+
                  ' ON ct.id = c.client_city_id '+
            ' LEFT JOIN states st '+
                  ' ON st.id = ct.state_id '+
                ' WHERE '+
                'c.judicial_process_exists = false AND '+ 
                'c.apprehension_search_exists = false AND '+
                'c.status in (\'service_provision\', \'reduction\') AND '+
                'c.active = true '+
                'AND c.contract_number_system is not null '+
                'AND c.reason_termination is null '+
                'AND c.debt_confession_date is null '+
                'AND c.contract_rescission_date is null '+
                'AND c.discharge_value is null '+
                'AND c.vehicle_selled is null '+
                'AND c.buy_date is null', (err, res) => {

  //console.log(res);
  res.rows.forEach(function(row){

    var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

    var params = {
      MessageBody: JSON.stringify({"cpf": row.cpf, "nome": row.nome, "uf": row.uf_novo, "contract_id": row.id_contract}),
      QueueUrl: process.env.AWS_QUEUE_LOCALIZACAO
    };
    
    sqs.sendMessage(params, function(err, data) {
      if (err) {
        console.log("Error " + err);
      } else {
        console.log("Enviando SQS processos: " + data.MessageId);
      }
    });


    if(row.uf_novo != row.uf){

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

    }

    if(row.uf_novo == 'DF' || row.uf_novo == 'GO'){
      var params = {
        MessageBody: JSON.stringify({"cpf": row.cpf, "nome": row.nome, "uf": row.uf_novo == 'DF'? 'GO': 'DF', "contract_id": row.id_contract}),
        QueueUrl: process.env.AWS_QUEUE_LOCALIZACAO
      };
      
      sqs.sendMessage(params, function(err, data) {
        if (err) {
          console.log("Error " + err);
        } else {
          console.log("Enviando SQS processos Duplicado (DF GO): " + data.MessageId);
        }
      });
    }

  });

  pool.end()
});
