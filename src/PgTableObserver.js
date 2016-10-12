var pgp = require('pg-promise')();
var _ = require('lodash');

import trigger_sql from './trigger_sql';

class PgTableObserver {
  constructor(connection, channel) {
    this.connection = connection;
    this.channel = channel;

    this.trigger_func = 'tblobs_' + channel;

    this.db = undefined; // Database connection
    this.notify_conn = undefined; // Notify connection
    this.payloads = {}; // Used by _processNotification
    this.table_cbs = {}; // table_name -> callbacks
  }

  // Private Methods

  async _init() {
    if(this.db) {
      throw new Error("Already initialized");
    }

    try {
      this.db = await pgp(this.connection);

      await this._initTriggerFunc();
      await this._initListener();
    }
    catch(err) {
      this.cleanup();
      throw err;
    }
  }

  async _initTriggerFunc() {
    let sql = trigger_sql(this.trigger_func, this.channel);

    await this.db.none('DROP FUNCTION IF EXISTS $1~() CASCADE', this.trigger_func);
    await this.db.none(sql);
  }

  async _initListener() {
    let notify_conn = await this.db.connect({direct: true});
    this.notify_conn = notify_conn;

    notify_conn.client.on('notification', info => {
      if(info.channel === this.channel) {
        var payload = this._processNotification(info.payload);

        // Complete payload?

        if(payload) {
          try {
            payload = JSON.parse(payload);
          }
          catch(err) {
            console.error("Error parsing payload", err.stack);
          }
          this._processPayload(payload);
        }
      }
    });

    await notify_conn.none('LISTEN $1~', this.channel);
  }

  _processNotification(payload) {
    // Assembles notification payload until complete
    // TODO Use regexp

    var arg_sep = [];

    while(arg_sep.length < 3) {
      var lastPos = arg_sep.length !== 0 ? arg_sep[arg_sep.length - 1] + 1 : 0;
      arg_sep.push(payload.indexOf(':', lastPos));
    }

    var hash = payload.slice(0, arg_sep[0]);
    var page_count = payload.slice(arg_sep[0] + 1, arg_sep[1]);
    var pageNr = payload.slice(arg_sep[1] + 1, arg_sep[2]);
    var message = payload.slice(arg_sep[2] + 1, arg_sep[3]);

    if(page_count > 1) {
      // Piece together multi-part messages

      if(!(hash in this.payloads)) {
        this.payloads[hash] = _.range(page_count).map(() => undefined );
      }
      this.payloads[hash][pageNr - 1] = message;

      if(this.payloads[hash].indexOf(undefined) !== -1) {
        return; // Must wait for full message
      }

      payload = this.payloads[hash].join('');

      delete this.payloads[hash];
    }
    else {
      // Payload small enough to fit in single message
      payload = message;
    }

    return payload;
  }

  _processPayload(payload) {
    // Do something with received payload
    // payload => { table, op, data, old_data }

    var table = payload.table;

    if (table in this.table_cbs) {
      // Parameter object for the callback

      var row = payload.data[0];

      var param = {
        table: table,
        row: row,
      };

      if(payload.op === 'UPDATE') {
        // old

        var old = payload.old_data[0];
        param.old = old;

        // changed fields (simple comparison)

        param.update = {};
        Object.keys(row).forEach(function(key) {
          if(row[key] !== old[key]) {
            param.update[key] = { from: old[key], to: row[key] };
          }
        });
      }
      else {
        param[payload.op.toLowerCase()] = true;
      }

      // Call the callbacks

      let callbacks = this.table_cbs[table];

      callbacks.forEach(callback => callback(param));
    }
  }

  //
  // Public Methods
  //

  async notify(tables, callback, _stopped) {
    // Check parameters

    if(typeof tables === 'string') {
      tables = [ tables ];
    }
    else if(!Array.isArray(tables)) {
      throw new TypeError('Tables missing');
    }

    if(typeof callback !== 'function') {
      throw new TypeError('Callback missing');
    }

    // initialize

    if(!this.db) {
      await this._init();
    }

    // Lowercase tables

    tables = tables.map(table => table.toLowerCase());

    // Often used properties

    let table_cbs = this.table_cbs;
    let db = this.db;

    // Check if we are already observing tables with same callback

    tables.forEach(table => {
      if(table in table_cbs && table_cbs[table].indexOf(callback) !== -1 ) {
        throw new Error(`Table already being observed with this callback: ${table}`);
      }
    });

    // Check for duplicate tables

    if ((new Set(tables)).size !== tables.length) {
      throw new Error('Tables contain duplicates.');
    }

    // Attach trigger to tables

    const drop_trigger_sql = 'DROP TRIGGER IF EXISTS $1~ ON $2~';

    let promises = tables.map(async table => {
      if(!(table in table_cbs)) {
        var trigger_name = `${this.channel}_${table}`;

        await db.none(drop_trigger_sql, [ trigger_name, table ]);
        await db.none(`
          CREATE TRIGGER $1~
          AFTER INSERT OR UPDATE OR DELETE ON $2~
          FOR EACH ROW EXECUTE PROCEDURE $3~()
        `, [ trigger_name, table, this.trigger_func ]);

        table_cbs[table] = [ callback ];
      }
      else {
        table_cbs[table].push(callback);
      }
    });

    await Promise.all(promises);

    return {
      stop: async () => {
        // Stop triggering

        if(_stopped) _stopped();

        // Stop notifications

        let promises = tables.map(async table => {
          _.pull(table_cbs[table], callback);

          if(table_cbs[table].length === 0) {
            delete table_cbs[table];

            var trigger_name = `${this.channel}_${table}`;

            await db.none(drop_trigger_sql, [ trigger_name, table ]);
          }
        });

        await Promise.all(promises);
      }
    };
  }

  async trigger(tables, triggers, callback, delay = 200) {
    // Check parameters

    if(typeof triggers !== 'function') {
      throw new TypeError('triggers missing');
    }

    if(typeof callback !== 'function') {
      throw new TypeError('Callback missing');
    }

    // Observe tables and handle notifications with timer

    let timer;
    let hit;

    let handle = await this.notify(tables,
      (change) => {
        // Timer running?

        if(!timer && triggers(change)) {
          // Callback

          callback();

          // Start timer for next hit

          hit = false;

          timer = setTimeout(() => {
            if(hit) {
              callback();
            }
            timer = undefined;
          }, delay);
        }
        else if(timer && !hit && triggers(change)) {
          // Hit the callback when timer fires
          hit = true;
        }
      },

      // Notifications were stopped, also stop our timer

      () => {
        if(timer) {
          clearTimeout(timer);
          timer = undefined;
        }
      }
    );

    return handle;
  }

  async cleanup(exit) {
    // Stop listener

    if(this.db) {
      if(this.notify_conn) {
        await this.notify_conn.done();
        this.notify_conn = undefined;
      }

      // Drop function, cascade to triggers

      await this.db.none('DROP FUNCTION IF EXISTS $1~() CASCADE', this.trigger_func);

      // Close db

      // await this.db.end();
      await pgp.end();
      this.db = undefined;

      // Cleanup rest

      this.payloads = {};
      this.table_cbs = {};
    }
  }

  if(exit) {
    process.exit();
  }
}

export default PgTableObserver;
