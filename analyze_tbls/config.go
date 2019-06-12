package analyze_tbls

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"io/ioutil"
)

type DbConfig struct {
	Host     string `toml:"host"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Port     string `toml:"port"`
}

type Config struct {
	Dbcfg DbConfig `toml:"db"`

	LogLever string `toml:"log-level"`
	LogPath  string `toml:"log-file"`

	WorkerCount int    `toml:"worker-count"`
	TableName   string `toml:"table-name"`

	StartDay string `toml:"start-day"`
	Period   int    `toml:"period"`
}

func defaultConfig(c *Config) {
	c.LogLever = "info"
	c.WorkerCount = 6
	c.LogPath = "./analyze.log"
}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {

	var c Config
	defaultConfig(&c)
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = NewConfig(&c, string(data))
	return &c, err
}

// NewConfig creates a Config from data.
func NewConfig(c *Config, data string) error {

	_, err := toml.Decode(data, c)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
