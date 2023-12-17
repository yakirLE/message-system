package config

import (
	"bytes"
	"log"
	"strings"

	"github.com/spf13/viper"
)

func New() *viper.Viper {
	v := viper.New()
	v.SetConfigFile("config.yaml")
	if err := v.ReadInConfig(); err != nil {
		if defErr := v.ReadConfig(bytes.NewBuffer(defaultConfig)); defErr != nil {
			log.Fatalf("default configuration is corrupted: %w", defErr)
		}
	}

	v.SetEnvPrefix("message_sender")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// workaround for https://github.com/spf13/viper/issues/761
	for _, k := range v.AllKeys() {
		val := v.Get(k)
		v.Set(k, val)
	}

	return v
}
