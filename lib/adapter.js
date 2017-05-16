'use strict';

const Pensuer = require('penseur');
const Hoek = require('hoek');
const Path = require('path');

const Adapter = function (params) {

    this.params = params || {};

    if (!this.params.database) {
        throw new Error('Database Connection params should be set');
    }

    if (typeof this.params.database !== 'function') {
        this.params.database = (cb) => cb(null, params.database);
    }
};

Adapter.prototype.getTemplatePath = function () {

    return Path.join(__dirname, 'migrationTemplate.js');
};

Adapter.prototype.connect = function (callback) {

    const self = this;

    self.params.database((err, database) => {

        if (err) {
            return callback(err);
        }


        const settings = Hoek.clone(database.connection);
        const tableName = database.migrationTable || 'migration';
        this.db = new Pensuer.Db(database.name, settings);
        this.db.table(self.params.tables);
        this.db.connect( (err) => {

            if (err) {
                return callback(err);
            }

            this.db.establish({ [tableName]: { purge: false } }, (err) => {

                if (err) {
                    return callback(err);
                }

                return callback(null, { db: self.db });
            });
        });
    });
};

Adapter.prototype.disconnect = function (callback) {

    this.db.close(() => {

        return callback();
    });
};

Adapter.prototype.getExecutedMigrationNames = function (callback) {

    const self = this;

    self.params.database((err, database) => {

        if (err) {
            return callback(err);
        }

        const tableName = database.migrationTable || 'migration';

        this.db[tableName].query({}, (err, migrations) => {

            if (err) {
                return callback(err);
            }

            if (migrations === null) {
                return callback(null, []);
            }

            callback(null, migrations.map((row) => {

                return row.name;
            }));
        });
    });
};

Adapter.prototype.markExecuted = function (name, callback) {

    const self = this;
    
    self.params.database((err, database) => {

        if (err) {
            return callback(err);
        }

        const tableName = database.migrationTable || 'migration';

        this.db[tableName].insert({ name: name, created: Date.now() }, (err, id) => {

            if (err) {
                return callback(err);
            }

            return callback(null);
        });
    });
};

Adapter.prototype.unmarkExecuted = function (name, callback) {

    const self = this;

    self.params.database((err, database) => {

        if (err) {
            return callback(err);
        }

        this.db[tableName].remove({ name: name }, (err) => {

            if (err) {
                return callback(err);
            }

            return callback(null);
        });
    });
};

module.exports = Adapter;
