#!/usr/bin/env node --use_strict

import 'source-map-support/register';

var pgp = require('pg-promise')();

import PgTableObserver from '../..';
// import PgTableObserver from '../PgTableObserver';


const connection = 'postgres://localhost/app';

async function start() {
  try {
    let db = await pgp(connection);

    let table_observer = new PgTableObserver(db, 'myappx');

    async function cleanupAndExit() {
      await table_observer.cleanup();
      await pgp.end();
      process.exit();
    }

    process.on('SIGTERM', cleanupAndExit);
    process.on('SIGINT', cleanupAndExit);

    // Show notifications

    let handle = await table_observer.notify(['test'], change => {
      console.log(change);
    });

    // Handle triggers

    // let handle = await table_observer.trigger(['test'],
    //   (change) => {
    //     console.log(change);
    //     return true;
    //   },
    //   () => {
    //     console.log('Trigger fired');
    //   }
    // );


    // ... when finished observing the table

    // await handle.stop();

    // ... when finished observing altogether

    // await table_observer.cleanup();
    // await pgp.end();
  }
  catch(err) {
    console.error(err);
  }
}

process.on('unhandledRejection', (err, p) => console.log(err.stack));

start();
