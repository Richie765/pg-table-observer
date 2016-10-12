# pg-observe-table
Observe PostgreSQL table for changes

# Usage

```javascript
import PgTableObserver from 'pg-table-observer';

// Create table_observer and cleanup and exit automatically

var table_observer = new PgTableObserver('postgres://localhost/app', 'myappx');

process.on('SIGTERM', () => table_observer.cleanup(true));
process.on('SIGINT', () => table_observer.cleanup(true));

// Do some observing

async function start() {
  try {
    // Multiple tables can be specified as an array

    let handle = await table_observer.notify('test', change => {
      console.log(change);
    });

    // Or trigger a callback when a condition is met

    let handle = await table_observer.trigger(condition, () => {
      console.log("condition was met")
    });

    // ... when finished observing the table

    await handle.stop();

    // ... when finished observing altogether

    await table_observer.cleanup();
  }
  catch(err) {
    console.error(err);
  }
}

// Show unhandled rejections

process.on('unhandledRejection', (err, p) => console.log(err.stack));

start();
```
