package config

import (
	"github.com/spf13/viper"
	"log"
	"testing"
)

func TestConfig(t *testing.T) {
	viper.AddConfigPath("./")     // path to look for the config file in
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	getString := viper.GetString("persist.aof.aof_file")
	log.Println(getString)
}
