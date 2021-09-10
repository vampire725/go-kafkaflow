package configutil

import (
	"github.com/Shopify/sarama"
	"strings"
)

/*
 * @Author: Gpp
 * @File:   config.go
 * @Date:   2021/9/10 3:41 下午
 */

type KafkaConfig struct {
	IsSecureMode    bool   `yaml:"isSecureMode"`
	Dsn             string `yaml:"dsn"`
	ConsumerGroupId string `yaml:"consumerGroupId"`
	KrbConfPath     string `yaml:"krbConfPath"`
	KeyTabPath      string `yaml:"keytabPath"`
	ServiceName     string `yaml:"serviceName"`
	Username        string `yaml:"username"`
	Realm           string `yaml:"realm"`
	DomainName      string `yaml:"domainName"`
}

func NewKafkaConsumer(kc KafkaConfig) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(strings.Split(kc.Dsn, ","), kc.ConsumerGroupId, NewKafkaConfig(kc))
}

func NewKafkaConfig(kc KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	if kc.IsSecureMode {
		config.Version = sarama.V1_1_0_0
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		config.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
			AuthType:           sarama.KRB5_KEYTAB_AUTH,
			KeyTabPath:         kc.KeyTabPath,
			KerberosConfigPath: kc.KrbConfPath,
			ServiceName:        kc.ServiceName,
			Username:           kc.Username,
			Realm:              kc.Realm,
		}
	} else {
		config.Version = sarama.V2_4_0_0
	}

	return config
}

func NewKafkaSyncProducer(kc KafkaConfig) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = 1e7
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	if kc.IsSecureMode {
		config.Version = sarama.V1_1_0_0
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI

		config.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
			AuthType:           sarama.KRB5_KEYTAB_AUTH,
			KeyTabPath:         kc.KeyTabPath,
			KerberosConfigPath: kc.KrbConfPath,
			ServiceName:        kc.ServiceName,
			Username:           kc.Username,
			Realm:              kc.Realm,
		}
	}

	return sarama.NewSyncProducer(strings.Split(kc.Dsn, ","), config)
}
