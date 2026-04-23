const assert = require('assert');
const Mysql = require('../..');
const config = require('../_lib/config')();
const query = require('../_lib/query')();

async function waitForError (ms = 50) {
  await new Promise(resolve => setTimeout(resolve, ms));
}

describe('connection retry on fatal error', () => {
  beforeEach(async () => {
    await query('DROP TABLE IF EXISTS foo');
    await query(`
      CREATE TABLE foo (
        id INT AUTO_INCREMENT,
        foo VARCHAR(100),
        PRIMARY KEY (id)
      )
    `);
    await query('INSERT INTO foo (foo) VALUES (?), (?)', ['pre1', 'pre2']);
  });

  afterEach(async () => {
    await query('DROP TABLE IF EXISTS foo');
  });

  it('retries query with fresh connection after fatal error outside transaction', async () => {
    const conn = new Mysql(config);

    try {
      const { result: [row] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      const firstConnId = row.id;

      await query(`KILL ${firstConnId}`);
      await waitForError();

      const { result } = await conn.rawQuery('SELECT * FROM foo');
      assert.strictEqual(result.length, 2);

      const { result: [row2] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      assert.notStrictEqual(row2.id, firstConnId, 'should be running on a new connection');
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('does not retry inside transaction and surfaces fatal error', async () => {
    const conn = new Mysql(config);

    try {
      await conn._begin();

      const { result: [row] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      const firstConnId = row.id;

      await query(`KILL ${firstConnId}`);
      await waitForError();

      let thrown;
      try {
        await conn.rawQuery('SELECT * FROM foo');
      } catch (err) {
        thrown = err;
      }

      assert(thrown, 'expected rawQuery to throw');
      assert.strictEqual(thrown.fatal, true, 'expected fatal error to be surfaced');
      assert.strictEqual(conn.inTransaction, false, 'inTransaction should be cleared after fatal error');
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('clears inTransaction flag after commit so subsequent queries can retry', async () => {
    const conn = new Mysql(config);

    try {
      await conn._begin();
      await conn.rawQuery('SELECT 1');
      await conn._commit();

      assert.strictEqual(conn.inTransaction, false);

      const { result: [row] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      const firstConnId = row.id;

      await query(`KILL ${firstConnId}`);
      await waitForError();

      const { result } = await conn.rawQuery('SELECT * FROM foo');
      assert.strictEqual(result.length, 2);
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('clears inTransaction flag after rollback', async () => {
    const conn = new Mysql(config);

    try {
      await conn._begin();
      await conn.rawQuery('SELECT 1');
      await conn._rollback();

      assert.strictEqual(conn.inTransaction, false);
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('transparently retries in-flight query killed mid-execution', async () => {
    const conn = new Mysql(config);

    try {
      const { result: [row] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      const firstConnId = row.id;

      const inFlight = conn.rawQuery('SELECT 1 AS n');
      await waitForError(30);
      await query(`KILL ${firstConnId}`);

      const { result } = await inFlight;
      assert.strictEqual(result[0].n, 1);
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('surfaces fatal error from in-flight query inside transaction', async () => {
    const conn = new Mysql(config);

    try {
      await conn._begin();
      const { result: [row] } = await conn.rawQuery('SELECT CONNECTION_ID() AS id');
      const firstConnId = row.id;

      const inFlight = conn.rawQuery('SELECT SLEEP(2)');
      await waitForError(30);
      await query(`KILL ${firstConnId}`);

      let thrown;
      try {
        await inFlight;
      } catch (err) {
        thrown = err;
      }
      assert(thrown, 'in-flight query should reject when connection killed');
      assert.strictEqual(conn.inTransaction, false, 'inTransaction should be cleared');
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });

  it('propagates non-fatal errors without retrying', async () => {
    const conn = new Mysql(config);

    try {
      let thrown;
      try {
        await conn.rawQuery('SELECT * FROM this_table_does_not_exist');
      } catch (err) {
        thrown = err;
      }

      assert(thrown, 'expected error');
      assert.notStrictEqual(thrown.fatal, true, 'non-existent table error should not be fatal');

      const { result } = await conn.rawQuery('SELECT * FROM foo');
      assert.strictEqual(result.length, 2);
    } finally {
      try { await conn.end(); } catch (_) { /* ignore */ }
    }
  });
});
