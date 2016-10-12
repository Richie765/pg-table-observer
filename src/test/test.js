#!/usr/bin/env node --use_strict

import 'source-map-support/register';

// import PgTableObserver from 'pg-table-observer';
import PgTableObserver from '../PgTableObserver';

var table_observer = new PgTableObserver('postgres://localhost/app', 'myappx');

process.on('SIGTERM', () => table_observer.cleanup(true));
process.on('SIGINT', () => table_observer.cleanup(true));

async function start() {
  try {
    // Show notifications

    // let handle = await table_observer.notify(['test'], change => {
    //   console.log(change);
    // });

    // Handle triggers

    let handle = await table_observer.trigger(['test'],
      (change) => {
        console.log(change);
        return true;
      },
      () => {
        console.log('Trigger fired');
      }
    );

    // ... when finished observing the table

    // await handle.stop();

    // ... when finished observing altogether

    // await table_observer.cleanup();

    // Change table after a delay

    setTimeout(async () => {
      let db = table_observer.db;
      console.log('Updating test');
      await db.any('UPDATE test SET b = CONCAT(b, "a")');
      console.log('Test updated');
    }, 3000);

  }
  catch(err) {
    console.error(err);
  }
}

process.on('unhandledRejection', (err, p) => console.log(err.stack));

start();
