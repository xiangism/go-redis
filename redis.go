package redis

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

func typeof(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

type Redis struct {
	conn redis.Conn
}

// 这里一定得用指针
func (r *Redis) Connect(ip string) bool {
	var err error
	r.conn, err = redis.Dial("tcp", ip,
		redis.DialConnectTimeout(time.Second*10), redis.DialReadTimeout(time.Second*10), redis.DialWriteTimeout(time.Second*10))
	//r.conn, err = redis.DialTimeout("tcp", ip)

	if err != nil {
		return false
	}
	return true
}

func (r *Redis) isConn() bool {
	if r.conn == nil {
		return false
	}
	return true
}

// 判断redis的返回为1或者为OK
func replyOK(reply interface{}) bool {
	i, ok := reply.(int64)
	if ok {
		return i == 1
	}
	s, ok := reply.(string)
	if ok {
		return strings.ToUpper(s) == "OK"
	}
	return false
}

func convInt(reply interface{}) int64 {
	i, ok := reply.(int64)

	if ok {
		return i
	}
	return 0
}

func convString(reply interface{}) string {
	s, ok := reply.(string)

	if ok {
		return s
	}

	bs, ok := reply.([]byte)

	if ok {
		return string(bs)
	}

	return ""
}

func convArr(reply interface{}) []string {
	rs := []string{}
	arrs, ok := reply.([]interface{})

	if ok {
		for _, item := range arrs {

			s, ok := item.([]byte)

			if ok {
				rs = append(rs, string(s))
			} else {
				rs = append(rs, "")
			}
		}

	}

	return rs
}

func convMap(reply interface{}) map[string]string {
	mp := make(map[string]string)
	arrs := convArr(reply)

	var key string
	for index, item := range arrs {

		if index%2 == 0 {
			key = item

		} else {
			mp[key] = item
			key = ""
		}
	}
	return mp
}

//////////////////////////////////////////////////////////
// Keys
func (r *Redis) Del(key string) bool {
	if !r.isConn() {
		return false
	}

	reply, err := r.conn.Do("del", key)

	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) Exists(key string) bool {
	if !r.isConn() {
		return false
	}

	reply, err := r.conn.Do("exists", key)
	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) Expire(key string, expire int64) bool {
	if !r.isConn() {
		return false
	}

	reply, err := r.conn.Do("expire", key, expire)

	if err != nil {
		return false
	}

	return replyOK(reply)
}

func (r *Redis) Keys(pattern string) []string {
	if !r.isConn() {
		return [] string{}
	}

	reply, err := r.conn.Do("keys", pattern)

	if err != nil {
		return []string{}
	}

	return convArr(reply)
}

func (r *Redis) Rename(key, newkey string) bool {
	if !r.isConn() {
		return false
	}

	reply, err := r.conn.Do("rename", key, newkey)

	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) Type(key string) string {
	if !r.isConn() {
		return ""
	}

	reply, err := r.conn.Do("type", key)

	if err != nil {
		return ""
	}

	s, ok := reply.(string)
	if ok {
		return s
	}
	return ""
	/*switch v := reply.(type) {
	case string:
		return v
	case [] byte:
		return string(v)
	}
	*/
}

//////////////////////////////////////////////////////////
// string
func (r *Redis) Get(key string) string {
	if !r.isConn() {
		return ""
	}

	reply, err := r.conn.Do("get", key)

	if err != nil {
		return ""
	}

	s, ok := reply.([]byte)

	if ok {
		return string(s)
	}
	return ""
}

func (r *Redis) MGet(keys ...string) []string {
	if !r.isConn() {
		return []string{}
	}

	s := make([]interface{}, len(keys))

	for i, v := range keys {
		s[i] = v
	}

	reply, err := r.conn.Do("mget", s...)

	if err != nil {
		fmt.Println("err, ", err)
		return []string{}
	}
	fmt.Printf("mget:(%v)\n", reply)
	return convArr(reply)
}

func (r *Redis) Set(key, value string) bool {
	if !r.isConn() {
		return false
	}

	if !r.isConn() {
		return false
	}

	reply, err := r.conn.Do("set", key, value)

	if err != nil {
		return false
	}

	switch v := reply.(type) {
	case string:
		if v == "OK" {
			return true
		}
	}
	return false
}

//////////////////////////////////////////////////////////
// list
func (r *Redis) Blpop(key string) string {
	if !r.isConn() {
		return ""
	}
	// TODO:
	return ""
}

func (r *Redis) Brpop(key string) string {
	if !r.isConn() {
		return ""
	}
	// TODO:
	return ""
}

func (r *Redis) LIndex(key string, index int64) string {
	if !r.isConn() {
		return ""
	}
	// TODO:
	return ""
}

func (r *Redis) LLen(key string) int64 {
	if !r.isConn() {
		return 0
	}
	// TODO
	return 0
}

func (r *Redis) LPop(key string) string {
	if !r.isConn() {
		return ""
	}
	// TODO
	return ""
}

func (r *Redis) LPush(key, value string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

func (r *Redis) LRange(key string, start int64, stop int64) []string {
	if !r.isConn() {
		return []string{}
	}
	// TODO:
	rs := []string{}
	return rs
}

func (r *Redis) LRem(key string, count int64, value string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

func (r *Redis) LSet(key string, index int64, value string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

func (r *Redis) RPop(key string) string {
	if !r.isConn() {
		return ""
	}
	// TODO:
	return ""
}

func (r *Redis) RPush(key, value string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

//////////////////////////////////////////////////////////
// set
func (r *Redis) SAdd(key, member string) bool {
	if !r.isConn() {
		return false
	}
	// TODO: test
	reply, err := r.conn.Do("sadd", key, member)

	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) SCard(key string) int64 {
	if !r.isConn() {
		return 0
	}
	// TODO: test
	reply, err := r.conn.Do("scard", key)

	if err != nil {
		return 0
	}
	return convInt(reply)
}

func (r *Redis) SIsMember(key, member string) bool {
	if !r.isConn() {
		return false
	}

	// TODO: test
	reply, err := r.conn.Do("sismember", key)

	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) SMembers(key string) []string {
	if !r.isConn() {
		return []string{}
	}

	reply, err := r.conn.Do("smembers", key)

	if err != nil {
		return []string{}
	}
	return convArr(reply)
}

func (r *Redis) SPop(key string, count int64) []string {
	if !r.isConn() {
		return []string{}
	}
	// TODO:
	return []string{}
}

func (r *Redis) SRem(key, member string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

//////////////////////////////////////////////////////////
// hash

func (r *Redis) HDel(key, field string) bool {
	if !r.isConn() {
		return false
	}
	reply, err := r.conn.Do("hdel", key, field)
	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) HExists(key, field string) bool {
	if !r.isConn() {
		return false
	}
	reply, err := r.conn.Do("hexists", key, field)
	if err != nil {
		return false
	}
	return replyOK(reply)
}

func (r *Redis) HGet(key, field string) string {
	if !r.isConn() {
		return ""
	}
	reply, err := r.conn.Do("hget", key, field)

	if err != nil {
		return ""
	}
	return convString(reply)
}

func (r *Redis) HGetall(key string) map[string]string {
	if !r.isConn() {
		return make(map[string]string)
	}

	reply, err := r.conn.Do("hgetall", key)

	if err != nil {
		return make(map[string]string)
	}
	return convMap(reply)
}

func (r *Redis) HMget(key string, fields ...string) map[string]string {
	if !r.isConn() {
		//return make(map[string]string)
		return map[string]string{}
	}

	s := make([]interface{}, len(fields)+1)

	s[0] = key
	for i, v := range fields {
		s[i+1] = v
	}

	reply, err := r.conn.Do("hmget", s...)

	if err != nil {
		fmt.Println("err have:", err)
		return make(map[string]string)
	}

	rs := convArr(reply)
	mp := make(map[string]string)

	for i, v := range fields {
		mp[v] = rs[i]
	}

	return mp
}

func (r *Redis) HMset(key string, fs map[string]string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

func (r *Redis) HSet(key, field, value string) bool {
	if !r.isConn() {
		return false
	}
	// TODO:
	return false
}

//////////////////////////////////////////////////////////
