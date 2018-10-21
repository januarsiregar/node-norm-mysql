const Connection = require('node-norm/connection');
const mysql2 = require('mysql2/promise');
const debug = require('debug')('node-norm-mysql:index');

const OPERATORS = {
  'eq': '=',
  'gt': '>',
  'lt': '<',
  'gte': '>=',
  'lte': '<=',
  'like': 'like',
};

class Mysql extends Connection {
  constructor (options) {
    super(options);

    let { host, user, password, database } = options;
    this.host = host;
    this.user = user;
    this.password = password;
    this.database = database;

    this.connPromise = mysql2.createConnection({ host, user, password, database });
    this.connPromise.then(conn => conn.on('error', this.onDbError.bind(this)));
  }

  getConnection () {
    if (!this.connPromise) {
      throw new Error('Connection already ended');
    }

    return this.connPromise;
  }

  onDbError (err) {
    debug('Database error', err);

    // Connection to the MySQL server is usually
    // lost due to either server restart
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      let { host, user, password, database } = this;
      this.connPromise = mysql2.createConnection({ host, user, password, database });
      this.connPromise.then(conn => conn.on('error', this.onError.bind(this)));
      return;
    }

    throw err;
  }

  async dbQuery (sql, params) {
    let conn = await this.getConnection();
    let [result, fields] = await conn.execute(sql, params);
    return { result, fields };
  }

  async insert (query, callback = () => {}) {
    let fieldNames = query.schema.fields.map(field => field.name);
    if (!fieldNames.length) {
      fieldNames = query.rows.reduce((fieldNames, row) => {
        for (let f in row) {
          if (fieldNames.indexOf(f) === -1) {
            fieldNames.push(f);
          }
        }
        return fieldNames;
      }, []);
    }

    let placeholder = fieldNames.map(f => '?');
    let sql = `INSERT INTO ${query.schema.name} (${fieldNames.join(',')}) VALUES (${placeholder})`;

    let changes = 0;
    await Promise.all(query.rows.map(async row => {
      let rowData = fieldNames.map(f => row[f]);

      let { result } = await this.dbQuery(sql, rowData);
      row.id = result.insertId;
      changes += result.affectedRows;

      callback(row);
    }));

    return changes;
  }

  async load (query, callback = () => {}) {
    let sqlArr = [ `SELECT * FROM ${mysql2.escapeId(query.schema.name)}` ];
    let [ wheres, data ] = this.getWhere(query);
    if (wheres) {
      sqlArr.push(wheres);
    }

    let orderBys = this.getOrderBy(query);
    if (orderBys) {
      sqlArr.push(orderBys);
    }

    if (query.limit >= 0) {
      sqlArr.push(`LIMIT ${query.limit}`);

      if (query.skip > 0) {
        sqlArr.push(`OFFSET ${query.skip}`);
      }
    }

    let sql = sqlArr.join(' ');

    let { result } = await this.dbQuery(sql, data);
    return result.map(row => {
      callback(row);
      return row;
    });
  }

  async delete (query, callback) {
    let [ wheres, data ] = this.getWhere(query);
    let sqlArr = [`DELETE FROM ${query.schema.name}`];
    if (wheres) {
      sqlArr.push(wheres);
    }

    let sql = sqlArr.join(' ');

    await this.dbQuery(sql, data);
  }

  getOrderBy (query) {
    let orderBys = [];
    for (let key in query.sorts) {
      let val = query.sorts[key];

      orderBys.push(`${mysql2.escapeId(key)} ${val ? 'ASC' : 'DESC'}`);
    }

    if (!orderBys.length) {
      return;
    }

    return `ORDER BY ${orderBys.join(', ')}`;
  }

  async update (query) {
    let keys = Object.keys(query.sets);

    let params = keys.map(k => query.sets[k]);
    let placeholder = keys.map(k => `${mysql2.escapeId(k)} = ?`);

    let [ wheres, data ] = this.getWhere(query);
    let sql = `UPDATE ${mysql2.escapeId(query.schema.name)} SET ${placeholder.join(', ')} ${wheres}`;
    let { result } = await this.dbQuery(sql, params.concat(data));

    return result.affectedRows;
  }

  getOr (query) {
    let wheres = [];
    let data = [];
    for (let i = 0; i < query.length; i++) {
      let key = Object.keys(query[i])[0];
      let value = Object.values(query[i])[0];
      let [ field, operator = 'eq' ] = key.split('!');
      if (operator === 'like') {
        value = '%' + value + '%';
      }
      data.push(value);
      wheres.push(`${field} ${OPERATORS[operator]} ?`);
    }
    return { where: `(${wheres.join(' OR ')})`, data: data };
  }

  getWhere (query) {
    let wheres = [];
    let data = [];
    for (let key in query.criteria) {
      let value = query.criteria[key];
      let [ field, operator = 'eq' ] = key.split('!');
      if (key === '!or') {
        let or = this.getOr(value);
        wheres.push(or.where);
        data = data.concat(or.data);
        continue;
      }

      // add by januar: for chek if operator like value change to %
      if (operator === 'like') {
        value = `%${value}%`;
      }

      data.push(value);
      wheres.push(`${mysql2.escapeId(field)} ${OPERATORS[operator]} ?`);
    }

    if (!wheres.length) {
      return [];
    }

    return [ `WHERE ${wheres.join(' AND ')}`, data ];
  }

  async _begin () {
    let conn = await this.getConnection();
    await conn.beginTransaction();
  }

  async _commit () {
    let conn = await this.getConnection();
    await conn.commit();
  }

  async _rollback () {
    let conn = await this.getConnection();
    await conn.rollback();
  }

  async count (query, useSkipAndLimit = false) {
    let sqlArr = [ `SELECT count(*) AS ${mysql2.escapeId('count')} FROM ${query.schema.name}` ];
    let [ wheres, data ] = this.getWhere(query);
    if (wheres) {
      sqlArr.push(wheres);
    }

    if (useSkipAndLimit) {
      if (query.length >= 0) {
        sqlArr.push(`LIMIT ${query.length}`);

        if (query.offset > 0) {
          sqlArr.push(`OFFSET ${query.offset}`);
        }
      }
    }

    let sql = sqlArr.join(' ');

    let db = await this.getDb();

    let row = await db.get(sql, data);
    return row.count;
  }

  async end () {
    let conn = await this.getConnection();
    this.connPromise = undefined;
    await conn.end();
  }
}

module.exports = Mysql;
