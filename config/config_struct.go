package config

type Persist struct {
	AOF
}

type AOF struct {
	AofFile        string
	TmpFile        string
	AofRewriteTime int64
}
