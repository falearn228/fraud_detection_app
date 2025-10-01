package config

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/IBM/sarama"
)

// KafkaConfig содержит конфигурацию для Kafka
type KafkaConfig struct {
	BootstrapServers       []string `yaml:"bootstrap_servers"`
	SecurityProtocol       string   `yaml:"security_protocol"`
	SSLTruststoreLocation  string   `yaml:"ssl_truststore_location"`
	SSLKeystoreLocation    string   `yaml:"ssl_keystore_location"`
	SSLKeystorePassword    string   `yaml:"ssl_keystore_password"`
	SSLKeyPassword         string   `yaml:"ssl_key_password"`
	SSLTruststorePassword  string   `yaml:"ssl_truststore_password"`
	SSLCALocation          string   `yaml:"ssl_ca_location"`
	SSLCertLocation        string   `yaml:"ssl_cert_location"`
	SSLKeyLocation         string   `yaml:"ssl_key_location"`
}

// NewKafkaConfig создает новую конфигурацию Kafka
func NewKafkaConfig(bootstrapServers []string) *KafkaConfig {
	return &KafkaConfig{
		BootstrapServers: bootstrapServers,
	}
}

// NewSaramaConfig создает конфигурацию Sarama для producer
func (kc *KafkaConfig) NewSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Проверяем, нужно ли включить TLS/SSL
	if kc.SecurityProtocol == "SSL" || kc.SecurityProtocol == "SASL_SSL" {
		tlsConfig, err := kc.createTLSConfig()
		if err == nil && tlsConfig != nil {
			config.Net.TLS.Enable = true
			config.Net.TLS.Config = tlsConfig
		}
	} else {
		config.Net.TLS.Enable = false
	}

	return config
}

// createTLSConfig создает конфигурацию TLS для подключения к Kafka
func (kc *KafkaConfig) createTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: false,
	}

	// Если указан CA сертификат, загружаем его
	if kc.SSLCALocation != "" {
		caCert, err := os.ReadFile(kc.SSLCALocation)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, &ValidationError{"failed to parse CA certificate"}
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Если указаны клиентские сертификаты, загружаем их
	if kc.SSLCertLocation != "" && kc.SSLKeyLocation != "" {
		cert, err := tls.LoadX509KeyPair(kc.SSLCertLocation, kc.SSLKeyLocation)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Validate проверяет корректность конфигурации
func (kc *KafkaConfig) Validate() error {
	if len(kc.BootstrapServers) == 0 {
		return ErrEmptyBootstrapServers
	}
	return nil
}

// ErrEmptyBootstrapServers ошибка пустых bootstrap серверов
var ErrEmptyBootstrapServers = &ValidationError{"bootstrap servers cannot be empty"}

// ValidationError ошибка валидации
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
