package config

import (
	"github.com/spf13/viper"
	"log"
)

var PersistConfig Persist

var EtcdConfig Etcd

var IsDev bool

var IsCluster bool

func init() {
	viper.AddConfigPath("/home/cirth9/go_file/r/reids-by-go/config") // path to look for the config file in
	viper.SetConfigName("config")                                    // name of config file (without extension)
	viper.SetConfigType("yaml")                                      // REQUIRED if the config file does not have the extension in the name
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	initPersist()
	initDev()
	initEtcd()
	log.Println(PersistConfig)
}

func initPersist() {
	PersistConfig.AofRewriteTime = viper.GetInt64("persist.aof.aof_rewrite_time")
	PersistConfig.TmpFile = viper.GetString("persist.aof.temp_dir")
	PersistConfig.AofFile = viper.GetString("persist.aof.aof_file")
}

func initDev() {
	IsDev = viper.GetBool("is_dev")
}

func initEtcd() {
	EtcdConfig.Addresses = viper.GetStringSlice("etcd_addresses")
	EtcdConfig.DialTimeOut = viper.GetInt("dial_time_out")
	EtcdConfig.Ttl = viper.GetInt64("ttl")
}
