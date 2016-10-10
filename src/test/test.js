#!/usr/bin/env node --use_strict

import 'source-map-support/register';

// import PgTableObserver from 'pg-table-observer';
import PgTableObserver from '../PgTableObserver';

var table_observer = new PgTableObserver('postgres://localhost/app', 'myappx');

process.on('SIGTERM', () => table_observer.cleanup(true));
process.on('SIGINT', () => table_observer.cleanup(true));

async function start() {
  try {
    let handle = await table_observer.observeTables(['test'], change => {
      console.log(change);
    });

    // ... when finished observing the table

    // setTimeout(async () => {
    //   console.log("Stopping");
    //   await handle.stop();
    //   await table_observer.cleanup();
    // }, 3000);

    // ... when finished observing altogether

//    await table_observer.cleanup();

  }
  catch(err) {
    console.error(err);
  }
}

process.on('unhandledRejection', (err, p) => console.log(err.stack));

start();
