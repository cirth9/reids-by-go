package database

import "time"

func getExpireTask(key string) string {
	return "expire:" + key
}

func (db *DB) Expire(key string, expireTime time.Time) {

}
