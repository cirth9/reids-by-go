package config

type Persist struct {
	AOF
}

type AOF struct {
	AofFile        string
	TmpFile        string
	AofRewriteTime int64
}

type Etcd struct {
	DialTimeOut int
	Ttl         int64
	Addresses   []string
}
