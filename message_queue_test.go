package gomemq

import (
	"testing"
)

func TestMessageQueue(t *testing.T) {
	// cases := []struct {
	// 	name           string
	// 	operation      func(mq *messageQueue) [][]byte
	// 	expectedReturn [][]byte
	// }{}
}

func BenchmarkPublish(b *testing.B) {
	cases := []struct {
		name     string
		subCount int
		msg      []byte
	}{
		{
			name:     "0.01kb-1sub",
			msg:      make([]byte, 10),
			subCount: 1,
		},
		{
			name:     "0.01kb-100sub",
			msg:      make([]byte, 10),
			subCount: 100,
		},
		{
			name:     "0.01kb-10_000sub",
			msg:      make([]byte, 10),
			subCount: 10_000,
		},
		{
			name:     "0.01kb-1_000_000sub",
			msg:      make([]byte, 10),
			subCount: 1_000_000,
		},
		{
			name:     "1kb-1sub",
			msg:      make([]byte, 1000),
			subCount: 1,
		},
		{
			name:     "1kb-100sub",
			msg:      make([]byte, 1000),
			subCount: 100,
		},
		{
			name:     "1kb-10_000sub",
			msg:      make([]byte, 1000),
			subCount: 10_000,
		},
		{
			name:     "1kb-1_000_000sub",
			msg:      make([]byte, 1000),
			subCount: 1_000_000,
		},
		{
			name:     "100kb-1sub",
			msg:      make([]byte, 100_000),
			subCount: 1,
		},
		{
			name:     "100kb-100sub",
			msg:      make([]byte, 100_000),
			subCount: 100,
		},
		{
			name:     "100kb-10_000sub",
			msg:      make([]byte, 100_000),
			subCount: 10_000,
		},
		{
			name:     "100kb-1_000_000sub",
			msg:      make([]byte, 100_000),
			subCount: 1_000_000,
		},
		{
			name:     "1mb-1sub",
			msg:      make([]byte, 1_000_000),
			subCount: 1,
		},
		{
			name:     "1mb-100sub",
			msg:      make([]byte, 1_000_000),
			subCount: 100,
		},
		{
			name:     "1mb-10_000sub",
			msg:      make([]byte, 1_000_000),
			subCount: 10_000,
		},
		{
			name:     "1mb-1_000_000sub",
			msg:      make([]byte, 1_000_000),
			subCount: 1_000_000,
		},
	}

	for _, c := range cases {
		cfg := Config{}
		mq := New(cfg)
		cfgTopic := ConfigTopic{
			Name: "test",
		}
		t, err := mq.Topic(cfgTopic)
		if err != nil {
			b.Fatalf(err.Error())
		}
		for i := 0; i < c.subCount; i++ {
			t.Subscribe(func(b []byte) error {
				return nil
			})
		}
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t.Publish(c.msg)
			}
		})
	}
}
