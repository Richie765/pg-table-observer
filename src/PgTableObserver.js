var _ = require('lodash');

class PgTableObserver {
  constructor(db, channel) {
    // TODO check parameters
    this.db = db;
    this.channel = channel;

    this.trigger_func = undefined;
    this.notify_conn = undefined;

    this.payloads = {}; // Used by _processNotification
    this.table_cbs = {}; // table_name -> callbacks
  }

  // Private Methods

  async _init() {
    if(this.trigger_func) {
      throw new Error("Already initialized");
    }

    this.trigger_func = 'tblobs_' + this.channel;

    try {
      await this._initTriggerFunc();
      await this._initListener();
    }
    catch(err) {
      this.cleanup();
      throw err;
    }
  }

  async _initTriggerFunc() {
    let trigger_sql = triggerSql(this.trigger_func, this.channel);

    await this.db.none('DROP FUNCTION IF EXISTS $1~() CASCADE', this.trigger_func);
    await this.db.none(trigger_sql);
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

  async notify(tables, callback) {
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

    if(!this.trigger_func) {
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

    let channel = this.channel;

    let promises = tables.map(async table => {
      if(!(table in table_cbs)) {
        var trigger_name = `${channel}_${table}`;

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
      async stop() {
        // Stop notifications

        let promises = tables.map(async table => {
          _.pull(table_cbs[table], callback);

          if(table_cbs[table].length === 0) {
            delete table_cbs[table];

            var trigger_name = `${channel}_${table}`;

            await db.none(drop_trigger_sql, [ trigger_name, table ]);
          }
        });

        await Promise.all(promises);
      }
    };
  }

  async trigger(tables, triggers, callback, options = {}) {
    // Check parameters

    if(typeof triggers !== 'function') {
      throw new TypeError('triggers missing');
    }

    if(typeof callback !== 'function') {
      throw new TypeError('Callback missing');
    }

    // Handle default options

    if(options.trigger_delay === undefined) options.trigger_delay = 200;
    if(options.reduce_triggers === undefined) options.reduce_triggers = true;
    if(options.trigger_first === undefined) options.trigger_first = true;

    // Observe tables and handle notifications with timer

    let timer;
    let hit;

    let handle = await this.notify(tables,
      (change) => {
        // Timer running?

        if(!timer && triggers(change)) {
          if(options.tigger_first) {
            callback();
            hit = false;
          }
          else {
            hit=true;
          }

          // Start timer to call callback when trigger hit

          timer = setTimeout(() => {
            if(hit) {
              callback();
            }
            timer = undefined;
          }, options.trigger_delay);
        }
        else if(timer && !hit && triggers(change)) {
          // Hit the callback when timer fires
          hit = true;
        }
        else if(!options.reduce_triggers) {
          triggers(change);
        }
      }
    );

    // Overloed stop

    let old_stop = handle.stop;
    handle.stop = () => {
      if(timer) {
        clearTimeout(timer);
        timer = undefined;
      }
      old_stop();
    };

    return handle;
  }

  async cleanup() {
    // Drop function, cascade to triggers

    if(this.trigger_func) {
      await this.db.none('DROP FUNCTION IF EXISTS $1~() CASCADE', this.trigger_func);
      this.trigger_func = undefined;
    }

    if(this.notify_conn) {
      await this.notify_conn.done();
      this.notify_conn = undefined;
    }

    // Cleanup rest

    this.payloads = {};
    this.table_cbs = {};
  }
}

function triggerSql(function_name, channel) {
  return `
    /*
     * Template for trigger function to send row changes over notification
     * Accepts 2 arguments:
     * funName: name of function to create/replace
     * channel: NOTIFY channel on which to broadcast changes
     */
    CREATE FUNCTION "${function_name}"() RETURNS trigger AS $$
      DECLARE
        row_data   RECORD;
        full_msg   TEXT;
        full_len   INT;
        cur_page   INT;
        page_count INT;
        msg_hash   TEXT;
      BEGIN
        IF (TG_OP = 'INSERT') THEN
          SELECT
            TG_TABLE_NAME AS table,
            TG_OP         AS op,
            json_agg(NEW) AS data
          INTO row_data;
        ELSIF (TG_OP  = 'DELETE') THEN
          SELECT
            TG_TABLE_NAME AS table,
            TG_OP         AS op,
            json_agg(OLD) AS data
          INTO row_data;
        ELSIF (TG_OP = 'UPDATE') THEN
          SELECT
            TG_TABLE_NAME AS table,
            TG_OP         AS op,
            json_agg(NEW) AS data,
            json_agg(OLD) AS old_data
          INTO row_data;
        END IF;

        SELECT row_to_json(row_data)::TEXT INTO full_msg;
        SELECT char_length(full_msg)       INTO full_len;
        SELECT (full_len / 7950) + 1       INTO page_count;
        SELECT md5(full_msg)               INTO msg_hash;

        FOR cur_page IN 1..page_count LOOP
          PERFORM pg_notify('${channel}',
            msg_hash || ':' || page_count || ':' || cur_page || ':' ||
            substr(full_msg, ((cur_page - 1) * 7950) + 1, 7950)
          );
        END LOOP;
        RETURN NULL;
      END;
    $$ LANGUAGE plpgsql;
  `;
}

export default PgTableObserver;
export { PgTableObserver };
