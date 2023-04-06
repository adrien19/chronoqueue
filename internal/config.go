package internal

type Config struct {
	Server struct {
		Host string `yaml:"host"`
		Port string `yaml:"port"`
	} `yaml:"server"`

	Redis struct {
		Dns  string `yaml:"dns"`
		Port string `yaml:"port"`
	} `yaml:"redis"`
}
