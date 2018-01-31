package redis

import (
	"testing"
	"strconv"
	"fmt"
)

// 比较两个map[string]string是否完全相同
func equalMap(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, v := range m1 {
		if m2[k] != v {
			return false
		}
	}
	for k, v := range m2 {
		if m1[k] != v {
			return false
		}
	}
	return true
}

func equalArr(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for index, item := range s1 {
		if item != s2[index] {
			return false
		}
	}
	return true
}

func equalSet(s1, s2 [] string) bool {
	if len(s1) != len(s2) {
		return false
	}

	m1 := map[string]string{}
	m2 := map[string]string{}

	for _, item := range s1 {
		m1[item] = ""
	}
	for _, item := range s2 {
		m2[item] = ""
	}
	return equalMap(m1, m2)
}

func getDb() Redis {
	//db := new(redis.Redis)
	db := Redis{}
	db.Connect(":6379")
	return db
}

func TestKey(t *testing.T) {
	db := getDb()
	key := "temp"

	//fmt.Println(db.Info())

	db.Del(key)
	db.Set(key, "t")

	if db.Type(key) != "string" {
		t.Errorf("type string error. %s\n", key)
	}

	db.Del(key)
	db.SAdd(key, "a")
	if db.Type(key) != "set" {
		t.Errorf("type set error. %s\n", key)
	}

	db.Del(key)
	db.RPush(key, "a")
	r := db.Type(key)
	if r != "list" {
		t.Errorf("type lsit error. %s\n", r)
	}

	db.Del(key)
	db.HSet(key, "k", "v")
	r = db.Type(key)
	if r != "hash" {
		t.Errorf("type hash error. %s\n", r)
	}

	db.Del(key)
	db.Set(key, "t")

	if !db.Exists(key) {
		t.Errorf("exists error. %s", key)
	}

	if !db.Del(key) {
		t.Errorf("del error")
	}
	if db.Del(key) {
		t.Errorf("del error2")
	}
	if db.Exists(key) {
		t.Errorf("exists2 error. %s", key)
	}

	key2 := "temp2"
	db.Del(key)
	db.Del(key2)
	db.Set(key, "a")

	if !db.Rename(key, key2) {
		t.Errorf("rename0 error")
	}

	if db.Exists(key) || !db.Exists(key2) {
		t.Errorf("rename1 error. %s, %s\n", key, key2)
	}

	if db.Get(key) != "" {
		t.Error("rename2 error. %s", key)
	}

	ks := db.Keys("*")
	dbsize := db.Dbsize()

	if int64(len(ks)) != dbsize {
		t.Error("keys dbsize, error, %d, %d\n", len(ks), dbsize)
	}
}

func TestExpire(t *testing.T) {
	db := getDb()

	key := "temp"
	db.Del(key)

	db.Set(key, "a")
	ttl := db.Ttl(key)
	if ttl != -1 {
		t.Errorf("expire ttl error")
	} else {
		fmt.Printf("ttl:(%d)\n", ttl)
	}

	db.Expire(key, 50)
	ttl = db.Ttl(key)

	if ttl < 30 {
		t.Errorf("expire2 expire error:%d\n", ttl)
	} else {
		fmt.Printf("ttl:(%d)\n", ttl)
	}

}

func TestGet(t *testing.T) {
	db := getDb()

	key := "key"
	db.Del(key)
	db.Set(key, "v1")

	if db.Get(key) != "v1" {
		t.Errorf("set get error:%s", key)
	}
}

func TestList(t *testing.T) {
	db := getDb()
	key := "list"
	db.Del(key)

	db.LPush(key, "c")
	db.LPush(key, "b")
	db.LPush(key, "a")

	if db.LLen(key) != 3 {
		t.Errorf("list len, push, error, %s\n", key)
	}

	if db.LIndex(key, 2) != "c" {
		t.Errorf("list index, error, %s\n", key)
	}

	if db.LPop(key) != "a" {
		t.Errorf("list pop, error, a")
	}
	if db.LPop(key) != "b" {
		t.Errorf("list pop, error, b")
	}

	db.LRem(key, 1, "c")
	if db.LLen(key) != 0 {
		t.Errorf("list lrem, error, c")
	}

	db.RPush(key, "a")
	db.RPush(key, "b")
	db.RPush(key, "c")
	db.RPush(key, "d")

	if db.LIndex(key, 3) != "d" {
		t.Errorf("lsit rpush, error d")
	}

	list := db.LRange(key, 0, 2)
	if !equalArr(list, []string{"a", "b", "c"}) {
		t.Errorf("lrange error, %v", list)
	}

	list = db.LRange(key, 0, -1)
	if !equalArr(list, [] string{"a", "b", "c", "d"}) {
		t.Errorf("lrange error, %v", list)
	}

	db.LSet(key, 2, "2")
	rs := db.LIndex(key, 2)
	if rs != "2" {
		t.Errorf("Lset error, %s", rs)
	}
}

func TestSet(t *testing.T) {
	db := getDb()
	key := "set"
	db.Del(key)

	db.SAdd(key, "a")
	db.SAdd(key, "b")
	db.SAdd(key, "a")
	db.SAdd(key, "c")

	i := db.SCard(key)
	if i != 3 {
		t.Errorf("scard, %d\n", i)
	}

	ss := db.SMembers(key)
	if !equalSet(ss, []string{"c", "a", "b"}) {
		t.Error("smembers error, %v\n", ss)
	}

	if !db.SIsMember(key, "a") {
		t.Errorf("sismember error, %s\n", "a")
	}

	db.SRem(key, "a")

	if db.SIsMember(key, "a") {
		t.Errorf("srem error, %s\n", "a")
	}

	s := db.SPop(key)
	if s == "" {
		t.Errorf("spop1, error, %s", s)
	}
	s = db.SPop(key)
	if s == "" {
		t.Errorf("spop2, error, %s", s)
	}
	s = db.SPop(key)
	if s != "" {
		t.Errorf("spop3, error, %s", s)
	}

	i = db.SCard(key)

	if i != 0 {
		t.Errorf("spop, error, %d", i)
	}
}

func TestHash(t *testing.T) {
	db := getDb()
	key := "hash"
	db.Del(key)

	db.HSet(key, "name", "xxxx")
	mp := map[string]string{"age": "30", "hobby": "run", "ke":"", "":"ve"}
	db.HMset(key, mp)

	mp["name"] = "xxxx"
	mp["ke"] = ""
	mp[""] = "ve"

	rp := db.HGetall(key)

	if !equalMap(rp, mp) {
		t.Errorf("hgetall error. mp:(%v), rp:(%v)", mp, rp)
	}

	r2 := db.HMget(key, "age", "name")
	mp2 := map[string]string{"name": "xxxx", "age": "30"}
	if !equalMap(r2, mp2) {
		t.Errorf("hmget error. r2:(%v), mp2:(%v)", r2, mp2)
	}

	field := ""

	if !db.HExists(key, field) {
		t.Errorf("hexist error")
	}

	if !db.HDel(key, field) {
		t.Errorf("hdel error")
	}

	if db.HExists(key, field) {
		t.Errorf("hexist error")
	}

	field = "name"

	if !db.HExists(key, field) {
		t.Errorf("hexist error")
	}

	if !db.HDel(key, field) {
		t.Errorf("hdel error")
	}

	if db.HExists(key, field) {
		t.Errorf("hexist error")
	}
}

func TestMap(t *testing.T) {
	m1 := map[string]string{}
	m2 := map[string]string{}

	for i := 0; i < 10; i++ {
		s := strconv.Itoa(i)
		m1[s] = s
		m2[s] = s
	}
	if !equalMap(m1, m2) {
		t.Errorf("map not equal, m1:(%v), m2:(%v)", m1, m2)
	}

	m1["a"] = "b"

	if equalMap(m1, m2) {
		t.Errorf("map not equal, m1:(%v), m2:(%v)", m1, m2)
	}
}
